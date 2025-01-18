MAX_LAST_MODIFIED = """SELECT MAX(modified) FROM content.film_work;"""

FETCH_FILMS_BATCH = """
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

FROM content.film_work fw
LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
LEFT JOIN content.genre g ON gfw.genre_id = g.id
LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
LEFT JOIN content.person p ON pfw.person_id = p.id
WHERE fw.modified >= $1
GROUP BY fw.id
ORDER BY fw.modified, fw.id
LIMIT $2 OFFSET $3
"""