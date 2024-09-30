from fastapi import FastAPI
#from fastapi.middleware.cors import CORSMiddleware
from app.routes import router
import logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
import time

# Настройка логирования в файл
logging.basicConfig(
    level=logging.ERROR,  # Уровень логирования ERROR
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log',  # Имя файла для логов
)

app = FastAPI()


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        inf = f"Запрос: {request.method} {request.url} Headers: {request.headers} Body: {await request.body()}"
        response = await call_next(request)
        
        # Если статус ответа - ошибка (код >= 400), записываем в лог
        if response.status_code >= 400:
            # Логгирование ошибки
            execution_time = time.time() - start_time
            logging.error(f"Ошибка: {response.status_code} {request.method} {request.url} Время выполнения: {execution_time:.2f} сек, {inf}")
        
        return response

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response

app.add_middleware(LoggingMiddleware)

# Разрешить запросы с любых источников (*)
'''app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)'''

app.include_router(router)
