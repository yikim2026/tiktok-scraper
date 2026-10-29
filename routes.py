"""The 41 TikTok endpoints. Every path is served with and without the
`/tiktok` prefix, so code generated against the hosted API on RapidAPI
(paths like /videos/details) runs unchanged against this server.

Params are validated by the same marshmallow schemas the hosted API uses
(tiktok/schemas.py): `user`, `video`, `hashtag`, `music`, `playlist`,
`collection`, `place` and `effect` each accept a bare value or a pasted
tiktok.com link.

[추가] 앱 전용 키 검사와 응답 캐시
  * API_KEY 환경 변수가 있으면 X-Api-Key 헤더가 같아야 응답한다 (없으면 검사하지 않는다).
    /, /health 는 Render 상태 확인용이라 키 없이 열어 둔다.
  * CACHE_TTL 의 경로는 같은 요청(경로 + 쿼리)이면 정해진 시간 동안 저장해 둔 응답을 준다.
    사용자가 늘어도 TikTok 으로 가는 요청은 경로·쿼리마다 그 시간에 한 번이다.
    같은 요청이 동시에 여러 개 와도 TikTok 에는 한 번만 묻는다.
    성공(200) 응답만 저장한다. 메모리에 두므로 서버가 잠들었다 깨면 비워진다.
"""
import hmac
import json
import os
import threading
import time

from bottle import request, response, route

from schema_fields import load_query
from scraper_errors import BadRequest, Blocked, NotFound, UpstreamError
from tiktok import ads, discovery, live, schemas, search, users, videos

# ---- [추가] 앱 전용 키 ------------------------------------------------------------------

API_KEY = os.environ.get("API_KEY") or None


def authorized():
    if not API_KEY:
        return True
    given = request.get_header("X-Api-Key") or ""
    return hmac.compare_digest(given.encode(), API_KEY.encode())


# ---- [추가] 응답 캐시 -------------------------------------------------------------------

# 경로 -> 저장 시간(초). 영상 주소가 몇 시간 뒤 만료되므로 길게 두지 않는다.
CACHE_TTL = {
    "/feed/trending": 5 * 60,       # 부를 때마다 새 묶음이 오는 피드라, 그동안은 모두 같은 묶음을 받는다
    "/hashtags/videos": 10 * 60,
    "/users/resolve": 24 * 60 * 60,
}
CACHE_MAX_ENTRIES = 300

_cache = {}          # 키 -> (만료 시각, 응답 본문)
_cache_lock = threading.Lock()
_key_locks = {}      # 키 -> Lock (같은 요청이 동시에 와도 TikTok 에는 한 번만)


def cache_key(path):
    query = query_dict()
    key = path + "?" + "&".join(f"{k}={query[k]}" for k in sorted(query))
    return key + "|app" if app_fields() else key


# ---- [추가] 앱용 작은 응답 -------------------------------------------------------------
# X-Fields: app 헤더가 오면 영상 목록에서 앱(MusicTown 인기영상)이 쓰는 항목만 보낸다.
# 화질별 영상 주소·자막·음악 정보 등을 빼면 응답이 약 350KB → 27KB (gzip 55KB → 6KB) 로 준다.
# 쿼리 파라미터가 아니라 헤더인 이유: 모르는 쿼리 파라미터는 스키마 검사에서 400 이 되지만
# 헤더는 무시되므로, 서버와 앱 중 어느 쪽을 먼저 배포해도 동작한다.

def app_fields():
    return (request.get_header("X-Fields") or "").lower() == "app"


def slim_video(v):
    video = v.get("video") or None
    return {
        "id": v.get("id"),
        "link": v.get("link"),
        "type": v.get("type"),
        "description": v.get("description"),
        "language": v.get("language"),
        "created_at": v.get("created_at"),
        "is_ad": v.get("is_ad"),
        "is_private": v.get("is_private"),
        "stats": {
            "play_count": (v.get("stats") or {}).get("play_count"),
            "like_count": (v.get("stats") or {}).get("like_count"),
        },
        "author": {"username": (v.get("author") or {}).get("username")},
        "cover": v.get("cover"),
        "video": {
            "duration_seconds": video.get("duration_seconds"),
            "cover": video.get("cover"),
        } if video else None,
    }


def slim(data):
    if not isinstance(data, dict) or not isinstance(data.get("videos"), list):
        return data
    out = {k: v for k, v in data.items() if k != "videos"}
    out["videos"] = [slim_video(v) for v in data["videos"] if isinstance(v, dict)]
    return out


def cache_get(key):
    with _cache_lock:
        entry = _cache.get(key)
        if entry and entry[0] > time.time():
            return entry[1]
        return None


def cache_put(key, ttl, body):
    now = time.time()
    with _cache_lock:
        for k in [k for k, (expires, _) in _cache.items() if expires <= now]:
            del _cache[k]
        while len(_cache) >= CACHE_MAX_ENTRIES:
            del _cache[min(_cache, key=lambda k: _cache[k][0])]
        _cache[key] = (now + ttl, body)


def lock_for(key):
    with _cache_lock:
        return _key_locks.setdefault(key, threading.Lock())


def cached_call(path, label, schema, fn):
    ttl = CACHE_TTL.get(path)
    if not ttl:
        return call(label, schema, fn)
    key = cache_key(path)
    body = cache_get(key)
    if body is None:
        with lock_for(key):
            body = cache_get(key)       # 기다리는 동안 다른 요청이 받아 두었을 수 있다
            if body is None:
                body = call(label, schema, fn)
                if response.status_code != 200:
                    return body         # 실패는 저장하지 않는다
                cache_put(key, ttl, body)
                response.set_header("X-Cache", "MISS")
                return body
    response.status = 200
    response.content_type = "application/json"
    response.set_header("X-Cache", "HIT")
    return body


# ---- 원래 코드 -------------------------------------------------------------------------

def json_response(data, status=200):
    response.status = status
    response.content_type = "application/json"
    return json.dumps(data, ensure_ascii=False)


def query_dict():
    """The query as unicode strings (bottle 0.12's .get() hands back latin-1
    decoded bytes, so a UTF-8 "Amélie" would arrive as "AmÃ©lie")."""
    return {key: request.query.getunicode(key) for key in request.query.keys()}


def call(label, schema, fn):
    """Validate, run, map errors: bad params -> 400, missing entity -> 404,
    transport/blocks -> 500."""
    data, error = load_query(schema, query_dict())
    if error:
        return json_response(error, 400)
    try:
        result = fn(**data)
        # [추가] 앱이 X-Fields: app 을 보내면 필요한 항목만
        return json_response(slim(result) if app_fields() else result)
    except ValueError as e:                # bad value
        return json_response({"error": str(e)}, 400)
    except BadRequest as e:                # upstream rejected the request (e.g. a bad cursor)
        return json_response({"error": f"tiktok rejected the request: {e}"}, 400)
    except NotFound as e:
        return json_response({"error": str(e) or "not found"}, 404)
    except Blocked as e:                   # silent empty body, 403 / 429
        return json_response({"error": f"tiktok blocked the request, retry later: {e}"}, 502)
    except UpstreamError as e:             # retries exhausted
        return json_response({"error": f"tiktok {label} failed: {e}"}, 502)
    except Exception as e:                 # parser surprises
        return json_response({"error": f"tiktok {label} failed: {type(e).__name__}: {e}"}, 500)


def mount(path, schema, fn):
    """Serve an endpoint function at /path and /tiktok/path."""
    label = path.strip("/")

    def handler():
        # [추가] 키 검사 → 캐시
        if not authorized():
            return json_response({"error": "unauthorized"}, 401)
        return cached_call(path, label, schema, fn)

    handler.__name__ = "tiktok_" + label.replace("/", "_").replace("-", "_")
    route(path, method="GET")(handler)
    route("/tiktok" + path, method="GET")(handler)


ENDPOINTS = [
    # videos
    ("/videos/details", schemas.VideoSchema, videos.get_details),
    ("/videos/media", schemas.VideoSchema, videos.get_media),
    ("/videos/comments", schemas.VideoPageSchema, videos.get_comments),
    ("/videos/comment-replies", schemas.CommentRepliesSchema, videos.get_comment_replies),
    ("/videos/transcript", schemas.TranscriptSchema, videos.get_transcript),
    ("/videos/related", schemas.VideoSchema, videos.get_related),
    ("/videos/resolve", schemas.VideoSchema, videos.resolve),
    # users
    ("/users/profile", schemas.UserSchema, users.get_profile),
    ("/users/videos", schemas.UserPostsSchema, users.get_posts),
    ("/users/reposts", schemas.UserVideosSchema, users.get_reposts),
    ("/users/followers", schemas.UserPageSchema, users.get_followers),
    ("/users/following", schemas.UserPageSchema, users.get_following),
    ("/users/playlists", schemas.UserPageSchema, users.get_playlists),
    ("/users/collections", schemas.UserPageSchema, users.get_collections),
    ("/users/resolve", schemas.UserSchema, users.resolve),
    # search
    ("/search/suggestions", schemas.SuggestionsSchema, search.get_suggestions),
    ("/search/videos", schemas.VideoSearchSchema, search.search_videos),
    ("/search/users", schemas.UserSearchSchema, search.search_users),
    ("/search/top", schemas.SearchSchema, search.search_top),
    ("/search/live", schemas.SearchSchema, search.search_live),
    # hashtags, sounds, playlists, collections, places, effects
    ("/hashtags/details", schemas.HashtagSchema, discovery.get_hashtag),
    ("/hashtags/videos", schemas.HashtagPostsSchema, discovery.get_hashtag_posts),
    ("/music/details", schemas.MusicSchema, discovery.get_music),
    ("/music/videos", schemas.MusicPostsSchema, discovery.get_music_posts),
    ("/playlists/details", schemas.PlaylistSchema, discovery.get_playlist),
    ("/playlists/videos", schemas.PlaylistPostsSchema, discovery.get_playlist_posts),
    ("/collections/videos", schemas.CollectionPostsSchema, discovery.get_collection_posts),
    ("/places/details", schemas.PlaceSchema, discovery.get_place),
    ("/places/videos", schemas.PlacePostsSchema, discovery.get_place_posts),
    ("/effects/details", schemas.EffectSchema, discovery.get_effect),
    # feeds
    ("/feed/trending", schemas.TrendingSchema, discovery.get_trending),
    ("/feed/explore", schemas.ExploreSchema, discovery.get_explore),
    ("/feed/explore/categories", schemas.EmptySchema, discovery.get_explore_categories),
    # live
    ("/live/details", schemas.UserSchema, live.get_room),
    # ads: Commercial Content Library + Creative Center top ads
    ("/ads/search", schemas.AdsSearchSchema, ads.search_library),
    ("/ads/details", schemas.AdSchema, ads.get_library_ad),
    ("/ads/advertisers", schemas.AdvertiserSearchSchema, ads.search_advertisers),
    ("/ads/countries", schemas.EmptySchema, ads.get_library_countries),
    ("/ads/top", schemas.TopAdsSchema, ads.get_top_ads),
    ("/ads/top/details", schemas.AdSchema, ads.get_top_ad),
    ("/ads/top/filters", schemas.EmptySchema, ads.get_top_ad_filters),
]

for _path, _schema, _fn in ENDPOINTS:
    mount(_path, _schema, _fn)


@route("/", method="GET")
@route("/health", method="GET")
def health():
    return json_response({"status": "ok", "endpoints": [p for p, _, _ in ENDPOINTS]})
