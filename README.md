В примере используется последняя версия AF (3.1.2)
Для запуска необходимо выполнить следующие шаги: 

1. Необходимо проверить наличие Docker, если его нет, необходимо установить.

2. Создаём виртуальное окружение
    - python -m venv venv

3. Устанавливаем зависимости 
    - pip install -r requirements.txt

4. Запускаем докер-файл
    - docker compose up -d

После запуска, у нас появляется:
1. Веб интерфейс AF по адресу: http://localhost:8080/, логин и пароль airflow
2. Веб интерфейс minio: http://localhost:9001/, логин и пароль minioadmin (при необходимости можно поменять в yaml)

После того, как докер поднялся, необходимо создать Buckets и Access Keys, в minio
Далее следует добавить переменные в AF.

![alt text](image.png)


Перед запуском дагов, необходимо создать структуру в БД:

CREATE SCHEMA ods;

create table ods.currency
(
	Cur_ID  varchar,
	Cur_ParentID  varchar,
	Cur_Code  varchar,
	Cur_Abbreviation  varchar,
	Cur_Name  varchar,
	Cur_Name_Bel  varchar,
	Cur_Name_Eng  varchar,
	Cur_QuotName  varchar,
	Cur_QuotName_Bel  varchar,
	Cur_QuotName_Eng  varchar,
	Cur_NameMulti  varchar,
	Cur_Name_BelMulti  varchar,
	Cur_Name_EngMulti  varchar,
	Cur_Scale  varchar,
	Cur_Periodicity  varchar,
	Cur_DateStart  varchar,
	Cur_DateEnd  varchar
);
