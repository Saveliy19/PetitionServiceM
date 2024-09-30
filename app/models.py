from pydantic import BaseModel, EmailStr
from datetime import datetime
from typing import List, Optional


class Emails(BaseModel):
    emails: List[EmailStr]

class Photo(BaseModel):
    filename: str
    content: str

# класс для создания новой заявки
class NewPetition(BaseModel):
    is_initiative: bool
    category: str
    petition_description: str
    petitioner_email: str
    address: str
    header: str
    region: str
    city_name: str
    photos: Optional[List[Photo]] = None

# класс для обновления статуса заявки
class PetitionStatus(BaseModel):
    id: int
    admin_id: int
    admin_city: str
    admin_region: str
    status: str
    comment: str
    

# класс для установки или отмены лайка
class Like(BaseModel):
    petition_id: int
    user_email: str

# класс для получения id пользователя от шлюза
class UserInfo(BaseModel):
    email: str

# класс с краткой информации о петиции
class PetitionWithHeader(BaseModel):
    id: int
    header: str
    status: str
    address: str
    date: str
    likes: int


# класс, используемый для передачи массива с краткой информацией по нескольким заявкам
class PetitionsByUser(BaseModel):
    petitions: List[PetitionWithHeader]


class AdminPetition(PetitionWithHeader):
    type: str

class AdminPetitions(BaseModel):
    petitions: List[AdminPetition]

# класс для получения данных о заявке, по которой нужно вернуть информацию
class PetitionToGetData(BaseModel):
    id: int

class Comment(BaseModel):
    date: str
    data: str

# класс для возврата полной информации по заявке
class PetitionData(BaseModel):
    id: int
    header: str
    is_initiative: bool
    category: str
    description: str
    status: str
    petitioner_email: str
    submission_time: str
    address: str
    likes_count: int
    region: str
    city_name: str
    comments: List[Comment]
    photos: List[str]

class City(BaseModel):
    region: str
    name: str

class CityWithType(City):
    is_initiative: bool

class SubjectForBriefAnalysis(City):
    period: str

class RegionForDetailedAnalysis(BaseModel):
    region_name: str
    city_name: str
    start_time: str
    end_time: str
    rows_count: int