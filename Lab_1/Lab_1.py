# Вариант 50

import sqlite3 as sql
import pandas as pd


class TaskClass:
    def __init__(self) -> None:
        self.conn = sql.connect("bookingdb.sqlite")
        self.crs = self.conn.cursor()
    
    def __del__(self):
        self.conn.close()
    
    def task_1(self):
        print('Задание 1:')
        
        sql_script = \
            '''
            SELECT
                guest_name AS 'Гость',
                room_name AS 'Комната',
                check_in_date AS 'Дата заселения',
                CAST((JulianDay(check_out_date) - JulianDay(check_in_date)) AS INT) + 1 AS 'Количество дней'
            FROM room_booking rb
            JOIN room r ON rb.room_id=r.room_id
            JOIN guest g ON rb.guest_id=g.guest_id
            WHERE guest_name LIKE '%а %'
            ORDER BY guest_name, room_name, 4 DESC;
            '''
        
        df = pd.read_sql(sql_script, self.conn)
        print(df)
        
    def task_2(self):
        print('Задание 2:')
        
        sql_script = \
            '''
            SELECT
                guest_name AS 'Фамилия',
                (
                    CASE
                        WHEN status_name='Занят' THEN 'Проживал(а)'
                        WHEN status_name='Забронирован' THEN 'Бронировал(а)'
                        WHEN status_name='Бронирование отменено' THEN 'Отменил(а) бронирование'
                        ELSE 'Не проживал(а)'
                    END
                ) AS 'Статус',
                (
                    CASE
                        WHEN COUNT(status_name)=0 THEN '-'
                        ELSE COUNT(*)
                    END
                ) AS 'Количество'
            FROM guest g
            LEFT JOIN room_booking rb ON g.guest_id=rb.guest_id
            LEFT JOIN status s ON rb.status_id=s.status_id
            GROUP BY guest_name, status_name
            ORDER BY guest_name, 2 DESC;
            '''
        
        df = pd.read_sql(sql_script, self.conn)
        print(df)
        
    def task_3(self):
        print('Задание 3:')
        
        sql_script = \
            '''
            SELECT
                Год,
                Месяц,
                MAX(Количество) AS Количество
            FROM (
                SELECT
                    strftime('%Y', check_in_date) AS Год,
                    ltrim(strftime('%m', check_in_date), '0') AS Месяц,
                    COUNT(*) AS Количество
                FROM room_booking
                GROUP BY 1, 2
            )
            GROUP BY 1
            ORDER BY 1, 2
            '''

        
        df = pd.read_sql(sql_script, self.conn)
        print(df)
        
    def task_4(self):
        print('Задание 4:')
        
        sql_script = \
            '''
            UPDATE room_booking
            SET status_id = (SELECT status_id FROM status WHERE status_name='Бронирование отменено' LIMIT 1)
            WHERE
                'П-1004'=(SELECT room_name FROM room r WHERE r.room_id=room_booking.room_id LIMIT 1)
                AND
                '2021-06-01'<=check_in_date
                AND
                'Жидкова Р.Л.'=(SELECT guest_name FROM guest g WHERE g.guest_id=room_booking.guest_id LIMIT 1);
    
            DELETE FROM service_booking
            WHERE room_booking_id=
                (SELECT room_booking_id FROM room_booking
                WHERE
                    'П-1004'=(SELECT room_name FROM room r WHERE r.room_id=room_booking.room_id LIMIT 1)
                    AND
                    '2021-06-01'<=check_in_date
                    AND
                    'Жидкова Р.Л.'=(SELECT guest_name FROM guest g WHERE g.guest_id=room_booking.guest_id LIMIT 1)
    );
            '''
        
        self.crs.executescript(sql_script)
        self.conn.commit()
        
        sql_script = \
            '''
            SELECT
                guest_name AS 'Гость',
                room_name AS 'Комната',
                check_in_date AS 'Дата заселения',
                status_name AS 'Статус'
            FROM room_booking rb
            JOIN room r ON rb.room_id=r.room_id
            JOIN guest g ON rb.guest_id=g.guest_id
            LEFT JOIN status s ON rb.status_id=s.status_id
            WHERE guest_name LIKE 'Жидкова %'
            ORDER BY guest_name, room_name;
            '''
        
        df = pd.read_sql(sql_script, self.conn)
        print(df)
        
    def task_5(self):
        print('Задание 5:')
        
        sql_script = \
            '''
            SELECT DISTINCT
                Гость,
                ROUND(SUM(Рейтинг) OVER (PARTITION BY Гость), 2) AS 'Рейтинг'
            FROM
                (SELECT
                    Гость,
                    CAST(Рейтинг AS REAL) / (SUM(Рейтинг) OVER (PARTITION BY Комната)) AS 'Рейтинг'
                FROM
                    (SELECT
                        guest_name AS 'Гость',
                        room_name AS 'Комната',
                        RANK() OVER(PARTITION BY rb.room_id ORDER BY check_in_date) AS 'Рейтинг'
                    FROM room_booking rb
                    JOIN room r ON rb.room_id=r.room_id
                    JOIN guest g ON rb.guest_id=g.guest_id)
                )
             ORDER BY 2 DESC
            '''
        
        df = pd.read_sql(sql_script, self.conn)
        print(df.to_string())


task = TaskClass()
task.task_4()
