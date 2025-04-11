import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import requests 

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}"  
    }
    
    payload = {
        "event_type": job,
        "client_payload": {
            "codeurl": codeurl,
            "dataseturl": dataseturl
        }
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 204:
        st.success(f"✅ Workflow '{job}' disparado correctamente!")
    else:
        st.error(f"❌ Error al disparar el workflow '{job}': {response.status_code}")
        try:
            st.write(response.json())
        except ValueError:
            st.write(response.text)

st.header("🚀 spark-submit Job")

col1, col2 = st.columns(2)

with col1:
    github_user  = st.text_input('👤 Github user', value='Iker186')
    github_repo  = st.text_input('📦 Github repo', value='social-media-project')
    spark_job    = st.text_input('🧪 Spark job', value='spark')

with col2:
    github_token = st.text_input('🔑 Github token', value='', type="password")  
    code_url     = st.text_input('🧾 Code URL', value='https://raw.githubusercontent.com/Iker186/social-media-project/main/spark_process.py')
    dataset_url  = st.text_input('📊 Dataset URL', value='https://raw.githubusercontent.com/Iker186/social-media-project/main/data/social_media.csv')

if st.button("▶️ POST spark submit"):
   post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

# POSTGRES
def post_to_kafka_postgres():
    url = "https://producer-postgres-latest.onrender.com/send-to-kafka-postgres"  # Cambia esta URL con la de tu productor en Render
    
    try:
        # Realizamos la petición POST
        response = requests.post(url)

        if response.status_code == 200:
            st.success("✅ Datos enviados a Kafka (Postgres) correctamente!")
        else:
            st.error(f"❌ Error al enviar los datos: {response.status_code}")
            st.write(response.text)
    
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Error de conexión: {e}")

if st.button("Enviar datos a postgres"):
    post_to_kafka_postgres()  # Llama a la función para enviar los datos

# MONGO
def post_to_kafka_mongo():
    url = "https://producer-mongo-latest.onrender.com/send-to-kafka-mongo"  # Cambia esta URL con la de tu productor en Render
    
    try:
        # Realizamos la petición POST
        response = requests.post(url)

        if response.status_code == 200:
            st.success("✅ Datos enviados a Kafka (Mongo) correctamente!")
        else:
            st.error(f"❌ Error al enviar los datos: {response.status_code}")
            st.write(response.text)
    
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Error de conexión: {e}")

if st.button("Enviar datos a mongo"):
    post_to_kafka_mongo()  # Llama a la función para enviar los datos