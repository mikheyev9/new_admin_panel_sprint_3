MAX_LAST_MODIFIED_PERSON = """SELECT MAX(modified) FROM content.person;"""

FETCH_PERSONS_BATCH = """
SELECT 
    p.id,
    p.modified,
    p.full_name
FROM content.person p
WHERE p.modified > $1 OR (p.modified = $1 AND p.id > $2)
ORDER BY p.modified, p.id
LIMIT $3;
"""

FETCH_FILMS_BY_PERSON = """
WITH film_ids AS (
    SELECT DISTINCT fw.id
    FROM content.film_work fw
    LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
    WHERE pfw.person_id = $1
     AND (COALESCE($3, NULL::uuid) IS NULL OR fw.id > $3::uuid)
    ORDER BY fw.id
    LIMIT $2
)
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