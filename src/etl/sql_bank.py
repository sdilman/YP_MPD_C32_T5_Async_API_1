SQL_LAST_INSERTED_PERSONS = f"SELECT id, modified " \
                            f"FROM content.person " \
                            f"WHERE modified > %s " \
                            f"ORDER BY modified " \
                            f"LIMIT %s;"

SQL_LAST_INSERTED_GENRES = f"SELECT id, modified " \
                           f"FROM content.genre " \
                           f"WHERE modified > %s " \
                           f"ORDER BY modified " \
                           f"LIMIT %s;"

SQL_PERSON_MOVIES = f"SELECT fw.id " \
                    f"FROM content.film_work fw " \
                    f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id " \
                    f"WHERE pfw.person_id IN %s " \
                    f"ORDER BY fw.modified "

SQL_GENRES_MOVIES = f"SELECT fw.id " \
                    f"FROM content.film_work fw " \
                    f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id " \
                    f"WHERE gfw.genre_id IN %s "

SQL_LAST_INSERTED_TIME_FILMWORKS = f"SELECT id, modified " \
                                   f"FROM content.film_work " \
                                   f"WHERE modified > %s " \
                                   f"ORDER BY modified " \
                                   f"LIMIT %s"

UNION_PERSON_GENRES = f"({SQL_PERSON_MOVIES}) UNION ({SQL_GENRES_MOVIES})"

ALL_MODIFIED_MOVIES = f"SELECT " \
                      f"fw.id as fw_id, " \
                      f"fw.title, " \
                      f"fw.description, " \
                      f"fw.rating, " \
                      f"COALESCE (" \
                      f"json_agg(" \
                      f"DISTINCT jsonb_build_object(" \
                      f"'person_role', pfw.role, " \
                      f"'person_id', p.id, " \
                      f"'person_name', p.full_name)" \
                      f") " \
                      f"FILTER (WHERE p.id is not null), " \
                      f"'[]') as persons, " \
                      f"array_agg(DISTINCT g.name) as genres " \
                      f"FROM content.film_work fw " \
                      f"LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id " \
                      f"LEFT JOIN content.person p ON p.id = pfw.person_id " \
                      f"LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id " \
                      f"LEFT JOIN content.genre g ON g.id = gfw.genre_id " \
                      f"WHERE fw.id IN ({UNION_PERSON_GENRES}) OR fw.id IN %s " \
                      f"GROUP BY fw.id;"