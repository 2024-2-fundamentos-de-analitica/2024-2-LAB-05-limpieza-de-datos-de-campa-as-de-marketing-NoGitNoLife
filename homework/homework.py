"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return


import pandas as pd
import zipfile
import os

# Ruta de entrada y salida
input_folder = "files/input/"
output_folder = "files/output/"

# Asegurarse de que la carpeta de salida exista
os.makedirs(output_folder, exist_ok=True)  # Crea la carpeta si no existe

def process_campaign_file(file_path):
    """
    Procesa un archivo CSV comprimido en formato ZIP, limpia los datos y genera los tres archivos CSV solicitados.
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Listar los archivos en el ZIP y procesar el primero
        file_name_in_zip = zip_ref.namelist()[0]
        
        # Leer el CSV comprimido directamente sin descomprimir
        with zip_ref.open(file_name_in_zip) as f:
            df = pd.read_csv(f)

    # Limpiar y transformar los datos

    # Client Data (client.csv)
    client_df = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    client_df['job'] = client_df['job'].str.replace('.', '').str.replace('-', '_')
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    client_df['education'] = client_df['education'].str.replace('.', '_')
    client_df['credit_default'] = client_df['credit_default'].map({'yes': 1, 'no': 0})
    client_df['mortgage'] = client_df['mortgage'].map({'yes': 1, 'no': 0})

    # Campaign Data (campaign.csv)
    campaign_df = df[['client_id', 'number_contacts', 'contact_duration', 
                      'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome']].copy()
    campaign_df['previous_outcome'] = campaign_df['previous_outcome'].map({'success': 1}).fillna(0)
    campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].map({'yes': 1}).fillna(0)
    
    # Crear la columna 'last_contact_date' en formato YYYY-MM-DD (solo año 2022)
    campaign_df['last_contact_date'] = df['month'] + ' ' + df['day'].astype(str) + ' 2022'
    campaign_df['last_contact_date'] = pd.to_datetime(campaign_df['last_contact_date'], format='%b %d %Y').dt.strftime('%Y-%m-%d')

    return client_df, campaign_df

def clean_campaign_data():
    """
    Procesa todos los archivos CSV comprimidos en la carpeta 'files/input/'.
    """
    all_client_data = []
    all_campaign_data = []
    
    # Iterar sobre todos los archivos ZIP en la carpeta 'files/input/'
    for file_name in os.listdir(input_folder):
        if file_name.endswith('.csv.zip'):
            file_path = os.path.join(input_folder, file_name)
            client_df, campaign_df = process_campaign_file(file_path)
            
            # Añadir los dataframes procesados a las listas
            all_client_data.append(client_df)
            all_campaign_data.append(campaign_df)

    # Concatenar todos los datos en un solo dataframe
    final_client_df = pd.concat(all_client_data, ignore_index=True)
    final_campaign_df = pd.concat(all_campaign_data, ignore_index=True)

    # Verificar si las columnas 'cons_price_idx' y 'euribor_three_months' están presentes
    if 'cons_price_idx' in final_client_df.columns and 'euribor_three_months' in final_client_df.columns:
        economics_df = final_client_df[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    else:
        # Si las columnas no están presentes, llenar con NaN
        economics_df = final_client_df[['client_id']].copy()
        economics_df['cons_price_idx'] = pd.NA
        economics_df['euribor_three_months'] = pd.NA
    
    # Guardar los archivos CSV en la carpeta de salida
    final_client_df.to_csv(os.path.join(output_folder, 'client.csv'), index=False)
    final_campaign_df.to_csv(os.path.join(output_folder, 'campaign.csv'), index=False)
    
    # Crear economics.csv
    economics_df.to_csv(os.path.join(output_folder, 'economics.csv'), index=False)



    


if __name__ == "__main__":
    clean_campaign_data()
