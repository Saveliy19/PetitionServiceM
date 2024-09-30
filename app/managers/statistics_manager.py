import asyncio

class StatisticsManager:
    def __init__(self, db):
                self.db = db
    
    async def get_most_popular_city_petition_by_period(self, region_name, city_name, period, is_initiative, rows):
        # получаем самую популярную категорию жалоб/иниициатив в указанном городе за определенный период
        query = f'''SELECT CATEGORY, count(*) FROM PETITION
                     WHERE SUBMISSION_TIME >= CURRENT_DATE - INTERVAL '{period}'
                     AND IS_INITIATIVE = $3
                     AND REGION = $1 AND CITY_NAME = $2
                     GROUP BY CATEGORY
                     ORDER BY COUNT(*) DESC
                     LIMIT $4;'''
        result = {record["category"]: record["count"] for record in await self.db.select_query(query, region_name, city_name, is_initiative, rows)}
        return result
        
    async def get_most_popular_region_petition_by_period(self, region, period, is_initiative, rows):
            # получаем самую популярную категорию жалоб/инициатив в указанном РЕГИОНЕ за определенный период
            query = f'''SELECT CATEGORY, count(*) FROM PETITION
                        WHERE SUBMISSION_TIME >= CURRENT_DATE - INTERVAL '{period}'
                        AND IS_INITIATIVE = $2
                        AND REGION = $1
                        GROUP BY CATEGORY
                        ORDER BY COUNT(*) DESC
                        LIMIT $3;'''
            result = {record["category"]: record["count"] for record in await self.db.select_query(query, region, is_initiative, rows)}
            return result
            
    async def get_city_petition_count_per_status_by_period(self, region, city, period, is_initiative):
            # получаем количество жалоб/инициатив на статус за период в указанном городе
            query = f'''SELECT PETITION_STATUS, COUNT(*) FROM PETITION
                    WHERE IS_INITIATIVE = $3
                    AND SUBMISSION_TIME >= CURRENT_DATE - INTERVAL '{period}'
                    AND REGION = $1 AND CITY_NAME = $2
                    GROUP BY PETITION_STATUS;'''
            result = {record["petition_status"]: record["count"] for record in await self.db.select_query(query, region, city, is_initiative)}
            return result

    async def get_region_petition_count_per_status_by_period(self, region, period, is_initiative):
            # получаем количество жалоб/инициатив на статус за период в указанном РЕГИОНЕ
            query = f'''SELECT PETITION_STATUS, COUNT(*) FROM PETITION
                    WHERE IS_INITIATIVE = $2
                    AND SUBMISSION_TIME >= CURRENT_DATE - INTERVAL '{period}'
                    AND REGION = $1
                    GROUP BY PETITION_STATUS;'''
            result = {record["petition_status"]: record["count"] for record in await self.db.select_query(query, region, is_initiative)}
            return result

    async def get_brief_subject_analysis(self, region_name, city_name, period):
            interval_mapping = {
                        "year": "1 year",
                        "month": "1 month",
                        "day": "1 day",
                        "week": "1 week"
                }
            period = interval_mapping[period]
            (
            most_popular_city_initiatives
            ,most_popular_city_complaints
            ,most_popular_region_initiatives
            ,most_popular_region_complaints
            ,city_complaints_count_per_status
            ,city_initiatives_count_per_status
            ,region_complaints_count_per_status
            ,region_initiatives_count_per_status
            ) = await asyncio.gather(
                self.get_most_popular_city_petition_by_period(region_name, city_name, period, True, 3)
                ,self.get_most_popular_city_petition_by_period(region_name, city_name, period, False, 3)
                ,self.get_most_popular_region_petition_by_period(region_name, period, True, 3)
                ,self.get_most_popular_region_petition_by_period(region_name, period, False, 3)
                ,self.get_city_petition_count_per_status_by_period(region_name, city_name, period, False)
                ,self.get_city_petition_count_per_status_by_period(region_name, city_name, period, True)
                ,self.get_region_petition_count_per_status_by_period(region_name, period, False)
                ,self.get_region_petition_count_per_status_by_period(region_name, period, True)
            )
            

            return {"most_popular_city_initiatives": most_popular_city_initiatives
                    ,"most_popular_city_complaints": most_popular_city_complaints
                    ,"city_initiatives_count_per_status": city_initiatives_count_per_status
                    ,"city_complaints_count_per_status": city_complaints_count_per_status
                    ,"most_popular_region_initiatives": most_popular_region_initiatives
                    ,"most_popular_region_complaints": most_popular_region_complaints
                    ,"region_initiatives_count_per_status": region_initiatives_count_per_status
                    ,"region_complaints_count_per_status": region_complaints_count_per_status
                    }
    
    async def get_full_statistics(self, region_name, city_name, start_time, end_time, rows_count):
            
            # количество жалоб на категорию в городе
            count_per_category_city_query = f'''SELECT CATEGORY, COUNT(*) AS COUNT_PER_CATEGORY
                                        FROM PETITION
                                        WHERE REGION = $1 AND CITY_NAME = $2
                                        AND (SUBMISSION_TIME BETWEEN $3 AND $4)
                                        AND IS_INITIATIVE = FALSE
                                        GROUP BY CATEGORY;'''
            
            # Список наиболее популярных инициатив в указанном городе
            most_popular_initiatives_city_query = f'''SELECT P.ID, P.HEADER, P.CATEGORY, P.SUBMISSION_TIME, COUNT(L.ID) AS LIKE_COUNT
                                            FROM PETITION P
                                            LEFT JOIN LIKES L ON P.ID = L.PETITION_ID
                                            WHERE P.IS_INITIATIVE = TRUE
                                            AND REGION = $1 AND CITY_NAME = $2
                                            AND (SUBMISSION_TIME BETWEEN $3 AND $4)
                                            GROUP BY P.ID
                                            ORDER BY LIKE_COUNT DESC
                                            LIMIT $5;'''
            
            # самые попклярные жалобы в указанном городе
            most_popular_complaints_city_query = f'''SELECT P.ID, P.HEADER, P.CATEGORY, P.SUBMISSION_TIME, COUNT(L.ID) AS LIKE_COUNT
                                            FROM PETITION P
                                            LEFT JOIN LIKES L ON P.ID = L.PETITION_ID
                                            WHERE P.IS_INITIATIVE = FALSE
                                            AND REGION = $1 AND CITY_NAME = $2
                                            AND (SUBMISSION_TIME BETWEEN $3 AND $4)
                                            GROUP BY P.ID
                                            ORDER BY LIKE_COUNT DESC
                                            LIMIT $5;'''
            
            # среднее количество жалоб на категорию в регионе
            count_per_category_region_query = f'''WITH CITY_COUNT AS (
                                            SELECT COUNT(DISTINCT CITY_NAME) AS CITY_COUNT
                                            FROM PETITION
                                            WHERE REGION = $1
                                            )

                                            SELECT CATEGORY, CAST(COUNT(*) AS FLOAT) / (SELECT CITY_COUNT FROM CITY_COUNT) AS COUNT_PER_CATEGORY
                                            FROM PETITION
                                            WHERE REGION = $1
                                            AND (SUBMISSION_TIME BETWEEN $2 AND $3)
                                            AND IS_INITIATIVE = FALSE
                                            GROUP BY CATEGORY;
                                            '''
            
            # среднее количество инициатив на категорию в регионе
            init_count_per_category_region_query = f'''WITH CITY_COUNT AS (
                                            SELECT COUNT(DISTINCT CITY_NAME) AS CITY_COUNT
                                            FROM PETITION
                                            WHERE REGION = $1
                                            )

                                            SELECT CATEGORY, CAST(COUNT(*) AS FLOAT) / (SELECT CITY_COUNT FROM CITY_COUNT) AS COUNT_PER_CATEGORY
                                            FROM PETITION
                                            WHERE REGION = $1
                                            AND (SUBMISSION_TIME BETWEEN $2 AND $3)
                                            AND IS_INITIATIVE = TRUE
                                            GROUP BY CATEGORY;
                                            '''
            
            # количество инициатив на категорию в городе
            init_count_per_category_city_query = f'''SELECT CATEGORY, COUNT(*) AS COUNT_PER_CATEGORY
                                        FROM PETITION
                                        WHERE REGION = $1 AND CITY_NAME = $2
                                        AND (SUBMISSION_TIME BETWEEN $3 AND $4)
                                        AND IS_INITIATIVE = TRUE
                                        GROUP BY CATEGORY;'''
            
            
            # количество инициатив в городе на день за указанный период
            init_count_per_day_query = f'''SELECT DATE(dates.d) AS DAY, COUNT(PETITION.SUBMISSION_TIME) AS INITIATIVES_COUNT
                                            FROM GENERATE_SERIES($2::TIMESTAMP, $3::TIMESTAMP, '1 day') AS dates(d)
                                            LEFT JOIN PETITION ON DATE(PETITION.SUBMISSION_TIME) = DATE(dates.d)
                                            AND PETITION.IS_INITIATIVE = TRUE
                                            AND PETITION.CITY_NAME = $1
                                            WHERE DATE(dates.d) BETWEEN $2 AND $3
                                            GROUP BY DATE(dates.d)
                                            ORDER BY DAY;
                                            '''
            # количество жалоб в день в городе за указанный период
            comp_count_per_day_query = f'''SELECT DATE(dates.d) AS DAY, COUNT(PETITION.SUBMISSION_TIME) AS COMPLAINTS_COUNT
                                            FROM GENERATE_SERIES($2::TIMESTAMP, $3::TIMESTAMP, '1 day') AS dates(d)
                                            LEFT JOIN PETITION ON DATE(PETITION.SUBMISSION_TIME) = DATE(dates.d)
                                            AND PETITION.IS_INITIATIVE = FALSE
                                            AND PETITION.CITY_NAME = $1
                                            WHERE DATE(dates.d) BETWEEN $2 AND $3
                                            GROUP BY DATE(dates.d)
                                            ORDER BY DAY;
                                            '''


            (
             cpc_city, mpi_city, mpc_city, cpc_reg,
             init_cpc_city, init_cpc_reg, icpd, ccpd
             ) = await asyncio.gather(self.db.select_query(count_per_category_city_query, region_name, city_name, start_time, end_time, rows_count),
                self.db.select_query(most_popular_initiatives_city_query, region_name, city_name, start_time, end_time, rows_count),
                self.db.select_query(most_popular_complaints_city_query, region_name, city_name, start_time, end_time, rows_count),
                self.db.select_query(count_per_category_region_query, region_name, start_time, end_time),
                self.db.select_query(init_count_per_category_city_query, region_name, city_name, start_time, end_time),
                self.db.select_query(init_count_per_category_region_query, region_name, start_time, end_time),
                self.db.select_query(init_count_per_day_query, city_name, start_time, end_time),
                self.db.select_query(comp_count_per_day_query))
            
            return {"count_per_category_city": {record["category"]: record["count_per_category"] for record in cpc_city},
                    "count_per_category_region": {record["category"]: record["count_per_category"] for record in cpc_reg},
                    "init_count_per_category_region": {record["category"]: record["count_per_category"] for record in init_cpc_reg},
                    "init_count_per_category_city": {record["category"]: record["count_per_category"] for record in init_cpc_city},
                    "init_per_day": {record["day"]: record["initiatives_count"] for record in icpd},
                    "comp_per_day": {record["day"]: record["complaints_count"] for record in ccpd},
                    "most_popular_city_initiatives": [dict(record) for record in mpi_city],
                    "most_popular_city_complaints": [dict(record) for record in mpc_city]}