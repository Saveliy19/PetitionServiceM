import os
import asyncio
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse

from app.models import (NewPetition, PetitionStatus, Like, UserInfo, PetitionToGetData,
                        PetitionData, CityWithType, SubjectForBriefAnalysis, City, 
                        Comment, RegionForDetailedAnalysis)

from app.managers.petition_manager import PetitionManager
from app.managers.statistics_manager import StatisticsManager

from app.db import DataBase
from app.config import host, port, user, database, password

db = DataBase(host, port, user, database, password)
petition_manager = PetitionManager(db)
statistics_manager = StatisticsManager(db)

router = APIRouter()

# маршрут для создания новой петиции
@router.post("/make_petition", status_code=status.HTTP_201_CREATED)
async def make_petition(petition: NewPetition):
    try:
        petition_id = await petition_manager.add_new_petition(petition)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    if petition.photos:
            await petition_manager.add_petition_photos(petition_id, petition.photos)
    return JSONResponse(content = petition_id)

# маршрут для обновления статуса заявки
@router.put("/update_petition_status", status_code=status.HTTP_200_OK)
async def update_petition_status(petition: PetitionStatus):
    if not (await petition_manager.check_city_by_petition_id(petition)):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='The admin does not have rights to this city')
    try:
        result, petitioner_emails = await asyncio.gather(petition_manager.update_petition_status(petition),
                                                        petition_manager.get_petitioners_email(petition))
    except:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    if result:
        return JSONResponse(content = petitioner_emails)
    else:
        raise HTTPException(status_code = status.HTTP_404_NOT_FOUND)


# маршрут для проверки лайка
@router.post("/check_like", status_code=status.HTTP_200_OK)
async def check_like(like: Like):
    try:
        result = await petition_manager.check_user_like(like)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return JSONResponse(content = {"result": result})

# маршрут для добавления лайка петиции
@router.put("/like_petition", status_code=status.HTTP_200_OK)
async def like_petition(like: Like):
    try:
        result = await petition_manager.like_petition(like)
        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Petition doesn't exists!")
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

# маршрут для получения списка заявок по id  пользователя
@router.post("/get_petitions", status_code=status.HTTP_200_OK)
async def get_petitions(user: UserInfo):
    try:
        petitions = await petition_manager.get_petitions_by_email(user.email)
    except Exception as e:
       raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return JSONResponse(content = {"petitions": petitions})

# маршрут для получения списка заявок по названию города
@router.post("/get_city_petitions", status_code=status.HTTP_200_OK)
async def get_city_petitions(city: CityWithType):
    try:
        petitions = await petition_manager.get_city_petitions(city)
    except Exception as e:
       raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return JSONResponse(content = {"petitions": petitions})

# маршрут для получения заявок, с которыми может работать админ
@router.post("/get_admins_city_petitions", status_code=status.HTTP_200_OK)
async def get_admins_city_petitions(city: City):
    try:
        petitions = await petition_manager.get_admin_petitions(city)
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return JSONResponse(content = {"petitions": petitions})

# маршрут для получения полных данных по заявке
@router.post('/get_petition_data', status_code=status.HTTP_200_OK)
async def get_petition_data(petition: PetitionToGetData):
    try:
        info, output_comments, photos = await asyncio.gather(petition_manager.get_full_petition_info(petition.id),
                                                      petition_manager.get_petition_comments(petition.id),
                                                      petition_manager.get_petition_photos(petition.id))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return PetitionData(id = info["id"], 
                        header = info["header"], 
                        is_initiative = info["is_initiative"], 
                        category = info["category"],
                        description = info["petition_description"],
                        status = info["petition_status"],
                        petitioner_email = info["petitioner_email"],
                        submission_time = info["submission_time"].strftime('%d.%m.%Y %H:%M'),
                        address = info["address"],
                        region = info["region"],
                        city_name = info["city_name"],
                        likes_count = info["likes_count"],
                        comments = output_comments,
                        photos = photos)

@router.get("/images/{image_path:path}")
async def get_image(image_path: str):
    return FileResponse(image_path)


# маршрут для получения краткой аналитики по населенному пункту
@router.post("/get_brief_analysis", status_code=status.HTTP_200_OK)
async def get_brief_analysis(subject: SubjectForBriefAnalysis):
    try:
        info = await statistics_manager.get_brief_subject_analysis(subject.region, subject.name, subject.period)
    except:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return JSONResponse(content = info)

#  маршрут для получения подробного анализа
@router.post("/get_detailed_analysis", status_code=status.HTTP_200_OK)
async def get_detailed_analysis(subject: RegionForDetailedAnalysis):
    try:
        info = await statistics_manager.get_full_statistics(subject.region_name, 
                                                            subject.city_name, 
                                                            subject.start_time, 
                                                            subject.end_time, 
                                                            subject.rows_count)
    except Exception as e:
       raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    return JSONResponse(content = info)