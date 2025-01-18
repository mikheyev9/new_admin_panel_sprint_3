MAX_LAST_MODIFIED_PERSON = """SELECT MAX(modified) FROM content.person;"""

FETCH_PERSONS_BATCH = """
SELECT 
    p.id,
    p.modified,
    p.full_name
FROM content.person p
WHERE p.modified >= $1
ORDER BY p.modified, p.id
LIMIT $2 OFFSET $3
"""

FETCH_FILMS_BY_PERSON = """
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
WHERE p.id = $1
GROUP BY fw.id
"""