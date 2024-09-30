import os
from app.config import PHOTOS_DIRECTORY
import base64

from app.models import PetitionStatus, NewPetition, Like, PetitionWithHeader, City, CityWithType, AdminPetition, Comment

class PetitionManager:
        def __init__(self, db):
                self.db = db

        # создание новой петиции
        async def add_new_petition(self, petition: NewPetition):
                
                query = '''INSERT INTO PETITION 
                (IS_INITIATIVE, CATEGORY, PETITION_DESCRIPTION, PETITIONER_EMAIL, ADDRESS, HEADER, REGION, CITY_NAME) 
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING ID;'''
                petition_id = await self.db.insert_returning(query, petition.is_initiative,
                                                                petition.category,
                                                                petition.petition_description,
                                                                petition.petitioner_email,
                                                                petition.address,
                                                                petition.header,
                                                                petition.region,
                                                                petition.city_name)
                return {"petition_id": f"{petition_id}"}
        
        # обновление статуса петиции
        async def update_petition_status(self, petition: PetitionStatus):
                query1 = f'''UPDATE PETITION
                             SET PETITION_STATUS = $1
                             WHERE ID = $2;'''
                query2 = f'''INSERT INTO COMMENTS (PETITION_ID, USER_ID, COMMENT_DESCRIPTION)
                             VALUES ($1, $2, $3);'''
                try:
                        await self.db.exec_many_query({
                        query1: [petition.status, petition.id],
                        query2: [petition.id, petition.admin_id, petition.comment]
                        })
                        return True
                except:
                        return False
        
        # получение списка пользователей, которые подписались под заявкой + сам заявитель
        async def get_petitioners_email(self, petition: PetitionStatus):
                query = f'''
                        SELECT PETITIONER_EMAIL AS email
                        FROM PETITION
                        WHERE ID = $1

                        UNION

                        SELECT USER_EMAIL AS email
                        FROM PETITION 
                        JOIN LIKES ON PETITION.ID = LIKES.PETITION_ID
                        WHERE PETITION.ID = $1;
                '''
                results = await self.db.select_query(query, petition.id)
                emails = [item["email"] for item in results]
                return {"petitioner_emails": emails}
        
        # установка лайка на петицию
        async def like_petition(self, like: Like):
                petition_query = '''SELECT * FROM PETITIONS WHERE ID = $1;'''
                existing_petition = await self.db.select_one(petition_query, like.petition_id)

                if not existing_petition:
                        return False

                query = '''SELECT * FROM LIKES WHERE PETITION_ID = $1 AND USER_EMAIL = $2;'''
                existing_like = await self.db.select_one(query, like.petition_id, like.user_email)

                if not existing_like:
                        query = '''INSERT INTO LIKES (PETITION_ID, USER_EMAIL) VALUES ($1, $2);'''
                        await self.db.exec_query(query, like.petition_id, like.user_email)
                else:
                        query = '''DELETE FROM LIKES WHERE PETITION_ID = $1 AND USER_EMAIL = $2;'''
                        await self.db.exec_query(query, like.petition_id, like.user_email)
                return

        # получаем список петиций пользователя по его email
        async def get_petitions_by_email(self, email):
                query = '''SELECT p.ID, p.HEADER, p.PETITION_STATUS, p.ADDRESS,
                         p.SUBMISSION_TIME, COUNT(l.petition_id) AS likes_count
                        FROM petition p
                        LEFT JOIN likes l ON p.ID = l.PETITION_ID
                        WHERE p.PETITIONER_EMAIL = $1 
                        GROUP BY p.ID;'''
                result = await self.db.select_query(query, email)
                petitions = [PetitionWithHeader(id=r["id"], 
                                        header=r["header"], 
                                        status=r["petition_status"], 
                                        address=r["address"], 
                                        date=r["submission_time"].strftime('%d.%m.%Y %H:%M'),
                                        likes = r["likes_count"]) for r in result]
                return petitions

        # проверка соответствия города петиции
        async def check_city_by_petition_id(self, petition: PetitionStatus):
                query = '''SELECT ($2, $3) IN
                         (SELECT REGION, CITY_NAME FROM PETITION WHERE id=$1)
                           as result;'''
                result = await self.db.select_query(query, petition.id, petition.admin_region, petition.admin_city)
                return result[0]["result"]

        # получаем список петиций и краткую информацию о них в указанном городе
        async def get_city_petitions(self, city: CityWithType):
                query = '''SELECT p.ID, p.HEADER, p.PETITION_STATUS, p.ADDRESS, p.SUBMISSION_TIME, COUNT(l.petition_id) AS likes_count
                FROM petition p
                LEFT JOIN likes l ON p.ID = l.PETITION_ID
                WHERE p.REGION = $1 
                AND p.CITY_NAME = $2 
                AND p.PETITION_STATUS != 'На модерации'
                AND p.IS_INITIATIVE = $3
                GROUP BY p.ID;
                '''
                result = await self.db.select_query(query, city.region, city.name, city.is_initiative)
                petitions = [PetitionWithHeader(id=r["id"], 
                                        header=r["header"], 
                                        status=r["petition_status"], 
                                        address=r["address"], 
                                        date=r["submission_time"].strftime('%d.%m.%Y %H:%M'),
                                        likes=r["likes_count"]) for r in result]
                return petitions

        # получаем список петиций с информацией о них в указанном городе, включая со статусом на модерации (доступно только админам)
        async def get_admin_petitions(self, city: City):
                query = '''SELECT p.ID, p.IS_INITIATIVE, p.HEADER, p.PETITION_STATUS, p.ADDRESS, p.SUBMISSION_TIME, COUNT(l.petition_id) AS likes_count
                FROM petition p
                LEFT JOIN likes l ON p.ID = l.PETITION_ID
                WHERE p.REGION = $1 
                AND p.CITY_NAME = $2
                GROUP BY p.ID;'''
                result = await self.db.select_query(query, city.region, city.name)
                petitions = [AdminPetition(id=r["id"],
                                        header=r["header"], 
                                        status=r["petition_status"], 
                                        address=r["address"], 
                                        date=r["submission_time"].strftime('%d.%m.%Y %H:%M'),
                                        likes=r["likes_count"],
                                        type = 'Жалоба' if r["is_initiative"] == False else 'Инициатива') for r in result]
                return result
        
        # получаем полную информацию о петиции
        async def get_full_petition_info(self, *args):
                query = '''SELECT p.*, COUNT(l.petition_id) AS likes_count
                FROM petition p
                LEFT JOIN likes l ON p.ID = l.PETITION_ID
                WHERE p.ID = $1
                GROUP BY p.ID;'''
                result = await self.db.select_one(query, *args)
                return result
        
        # получаем комментарии к петиции
        async def get_petition_comments(self, petition_id):
                query = '''SELECT SUBMISSION_TIME, COMMENT_DESCRIPTION FROM COMMENTS WHERE PETITION_ID = $1;'''
                comments = await self.db.select_query(query, petition_id)
                output_comments = [Comment(date=c["submission_time"].strftime('%d.%m.%Y %H:%M'), data=c["comment_description"]) for c in comments]
                return output_comments
        # проверяем лайк пользователя на записи
        async def check_user_like(self, like: Like):
                query = '''SELECT * FROM LIKES WHERE PETITION_ID = $1 AND USER_EMAIL = $2;'''
                existing_like = await self.db.select_one(query, like.petition_id, like.user_email)
                if not existing_like:
                        return False
                return True
        
        # добавляем фотографии петиции
        async def add_petition_photos(self, petition_id, photos):
                folder_path = PHOTOS_DIRECTORY + f"{petition_id}"
                os.mkdir(folder_path)
                for p in photos:
                        with open(PHOTOS_DIRECTORY + f'{petition_id}/{p.filename}', 'wb') as f:
                                f.write(base64.b64decode(p.content))

                query = '''INSERT INTO PHOTO_FOLDER (PETITION_ID, FOLDER_PATH) VALUES ($1, $2);'''
                await self.db.exec_query(query, petition_id, folder_path)

        # получаем фотографии петиции
        async def get_petition_photos(self, petition_id):
                photos = []
                query = f'''SELECT FOLDER_PATH FROM PHOTO_FOLDER WHERE PETITION_ID = $1;'''
                folder_path = (await self.db.select_one(query, petition_id))["folder_path"]
                for filename in os.listdir(folder_path):
                        file_path = os.path.join(folder_path, filename)
                        photos.append('http://127.0.0.1:8002/images/' +file_path)
                return photos