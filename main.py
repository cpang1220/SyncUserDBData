import requests
import psycopg2
import datetime


def get_user_data(user_id):
    url = "userapi"
    r_get = requests.get(url + str(user_id))
    status_code = r_get.status_code
    if status_code == 200:
        user_data = r_get.json()
    else:
        user_data = 'NA'
    return user_data


def sync_user_data_db(user_id, user_name, user_friends):
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="P@ssw0rd",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")
        cursor = connection.cursor()
        query = "select * from public.user where id = %s"
        cursor.execute(query, (user_id,))
        user_record = cursor.fetchone()
        if user_record is not None:
            user_name_db = user_record[1]
            if user_name.lower() != user_name_db.lower():
                update_query = """Update public.user set name = %s where id = %s"""
                cursor.execute(update_query, (user_name, user_id))
                connection.commit()
        else:
            user_insert_query = """ INSERT INTO public.user (id, name) VALUES (%s,%s)"""
            user_record_insert = (user_id, user_name)
            cursor.execute(user_insert_query, user_record_insert)
            connection.commit()

        user_friends_query = 'select * from public.friend where "from" = %s'
        cursor.execute(user_friends_query, (user_id,))
        user_friends_records = cursor.fetchall()
        for row in user_friends_records:
            if row[1] not in user_friends:
                sql_delete_query = """Delete from public.friend where "from" = %s and "to" = %s"""
                cursor.execute(sql_delete_query, (user_id, row[1]))
                connection.commit()

        for friend_id in user_friends:
            user_friends_detail_query = 'select * from public.friend where "from" = %s and "to" = %s'
            cursor.execute(user_friends_detail_query, (user_id, friend_id))
            user_friends_detail_record = cursor.fetchone()
            if user_friends_detail_record is None:
                user_friends_insert_query = """ INSERT INTO public.friend ("from", "to") VALUES (%s,%s)"""
                friend_record_insert = (user_id, friend_id)
                cursor.execute(user_friends_insert_query, friend_record_insert)
                connection.commit()
    except (Exception, psycopg2.Error) as error:
        print("PostgreSQL database error: ", error)

    finally:
        if(connection):
            cursor.close()
            connection.close()


def sync_user_data():
    for i in range(11):
        user_data = get_user_data(i)
        if user_data != 'NA':
            user_name = user_data['name']
            user_friends = user_data['friends']
            sync_user_data_db(i, user_name, user_friends)


def main():
    sync_user_data()
    dt_string = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    print("user data database sync script execution completed at ", dt_string)


if __name__ == "__main__":
    main()

