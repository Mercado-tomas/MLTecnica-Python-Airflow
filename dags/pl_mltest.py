
## importaciones necesarias
from asyncio import tasks
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import requests
import csv
from dotenv import load_dotenv
import os
import smtplib
from email.mime.text import MIMEText

## seteo de db
DB_PARAMS = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": "5432"
}
# cargamos variables de entorno
load_dotenv()

# path para el csv en docker
CSV_PATH_TEST = "./data/items.csv" #realizar prueba de carga
CSV_PATH = "/opt/airflow/data/items.csv" #ruta del contenedor para airflow

# funcion para scrap y guardar en csv
def scrap_to_csv():
    url = "https://api.mercadolibre.com/sites/MLA/search?category=MLA1577#json"
    response = requests.get(url)
    data = response.json()
    items = data["results"][:50] #50 más relevantes

    # campos requeridos
    fieldnames = ["id","site_id","title","price","sold_quantity","thumbnail","create_date"]
    try:
        # escribimos en csv
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in items:
                writer.writerow(
                    {
                        ## usamos item.get para que no tire error si es null o vacio
                        "id": item.get("id", ""),
                        "site_id": item.get("site_id", ""),
                        "title": item.get("title", ""),
                        "price": float(item.get("price", 0)),
                        "sold_quantity": int(item.get("sold_quantity", 10)),
                        "thumbnail": item.get("thumbnail", ""),
                        "create_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                )
        print(f"CSV guardado en: {CSV_PATH}")                
    except Exception as e:
        raise Exception(f"Error al escribir el CSV: {str(e)}")
# función para leer el csv y subirlo a la db
def csv_to_db():
    items_data = []
    try:
        with open(CSV_PATH, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                items_data.append(row)
        print(f"Nro de items leídos del CSV: {len(items_data)}")    
    except Exception as e:
        raise Exception (f"Error al leer el CSV: {str(e)}")
    try:
        # conexión a db
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        # crear tabla si no existe
        cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mercadolibre_items(
            id VARCHAR(50) PRIMARY KEY,
            site_id VARCHAR(10),
            title TEXT,
            price FLOAT,
            sold_quantity INT,
            thumbnail TEXT,
            create_date TIMESTAMP
        )
        """ 
        )

        # insertar o actualizar datos
        for item in items_data:
            cur.execute(
        """
            INSERT INTO mercadolibre_items (id, site_id, title, price, sold_quantity, thumbnail, create_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                site_id = EXCLUDED.site_id,
                title = EXCLUDED.title,
                price = EXCLUDED.price,
                sold_quantity = EXCLUDED.sold_quantity,
                thumbnail = EXCLUDED.thumbnail,
                create_date = EXCLUDED.create_date
        """,(item["id"],item["site_id"],item["title"],float(item["price"]), int(item["sold_quantity"]), item["thumbnail"],item["create_date"])
            )

        # ejectuamos la query
        conn.commit()
        cur.close()
        conn.close()
        print("Datos subidos a la DB.")
    except Exception as e:
        raise Exception(f"Error al interactuar con la DB: {str(e)}")
## funcion control y envío de email
def check_and_send_email():
    try:
        #conectar a la db
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        # query para seleccionar y filtrar
        cur.execute(
        """
            SELECT title,price,sold_quantity
            FROM public.mercadolibre_items
            WHERE (price * sold_quantity) > 70000
        """ 
        )
        high_value_items = cur.fetchall() #carga todos los datos de la query
        print(f"Items con calor mayor a 7M: {len(high_value_items)}")
    
        # cerramos
        cur.close()
        conn.close()

        # validamos si hay items
        if high_value_items:
            sender = os.getenv("EMAIL_SENDER")
            receiver = os.getenv("EMAIL_RECEIVER")
            password = os.getenv("EMAIL_PASSWORD")

            #creamos el mensaje
            body = "Items con ganancias mayor a 7 millones: \n\n"    
            for item in high_value_items:
                title, price, sold_quantity = item
                total_value = price * sold_quantity
                body += f"- {title}: ${total_value:,.2f} (Precio: ${price:,.2f}, Vendidos: {sold_quantity}\n)"

            # cuerpo de email
            msg = MIMEText(body)
            msg["Subject"] = "Alerta: Items de alto valor en Mercado Libre."
            msg["From"] = sender
            msg["To"] = receiver

            # enviamos el email
            with smtplib.SMTP("smtp.gmail.com", 587) as server:
                server.starttls()
                server.login(sender, password)
                server.send_message(msg)
            print(f"Email enviado a {receiver} con {len(high_value_items)} items.")    
        else:
            print("No hay items con valor mayor a 7 millones, no se envía email.")        



    except Exception as e:
        raise Exception(f"Error al enviar el email: {str(e)}")    


# definimos eldag
with DAG(
    dag_id="mercadolibre_técnica",
    start_date=datetime(2025, 3, 4),
    #schedule_interval="@daily", #ejecuta diario
    schedule_interval=None, # se debe de ejecutar manualmente desde ui de airflow
    catchup=False #evita ejecucaciones retroactivas
) as dag:
    scrap_task = PythonOperator(
        task_id = "scrap_to_csv_task",
        python_callable=scrap_to_csv
    )

    db_task = PythonOperator(
        task_id = "csv_to_db_task",
        python_callable=csv_to_db
    )

    email_task = PythonOperator(
        task_id = "email_alert_task",
        python_callable=check_and_send_email
    )

##establecemos el orden de ejecución
scrap_task >> db_task >> email_task



## prbamos la carga al csv
if __name__ == "__main__":
    scrap_to_csv()
    csv_to_db()
    check_and_send_email()