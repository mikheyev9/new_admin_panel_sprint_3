MAX_LAST_MODIFIED_GENRE = """SELECT MAX(modified) FROM content.genre;"""

FETCH_GENRES_BATCH = """
SELECT 
    g.id,
    g.modified,
    g.name
FROM content.genre g
WHERE g.modified > $1 OR (g.modified = $1 AND g.id > $2)
ORDER BY g.modified, g.id
LIMIT $3;
"""

FETCH_FILMS_BY_GENRE = """
WITH film_ids AS (
    -- Получаем ID фильмов для данного жанра
    SELECT DISTINCT fw.id
    FROM content.film_work fw
    LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
    WHERE gfw.genre_id = $1
      AND (COALESCE($3, NULL::uuid) IS NULL OR fw.id > $3::uuid)
    ORDER BY fw.id
    LIMIT $2
)
-- Получаем полные данные по ID фильмов
SELECT 
    fw.id,
    fw.modified,
    fw.title,
    fw.description,
    fw.rating AS imdb_rating,
    ARRAY_AGG(DISTINCT g.name) AS genres,
    jsonb_agg(
        DISTINCT jsonb_build_object(
            'id', p.id::text,
            'name', p.full_name
        )
    ) FILTER (WHERE pfw.role = 'actor') AS actors,
    jsonb_agg(
        DISTINCT jsonb_build_object(
            'id', p.id::text,
            'name', p.full_name
        )
    ) FILTER (WHERE pfw.role = 'director') AS directors,
    jsonb_agg(
        DISTINCT jsonb_build_object(
            'id', p.id::text,
            'name', p.full_name
        )
    ) FILTER (WHERE pfw.role = 'writer') AS writers
FROM film_ids
JOIN content.film_work fw ON fw.id = film_ids.id
LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
LEFT JOIN content.genre g ON gfw.genre_id = g.id
LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
LEFT JOIN content.person p ON pfw.person_id = p.id
GROUP BY fw.id
ORDER BY fw.id
"""
