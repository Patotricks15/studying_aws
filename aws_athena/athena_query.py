import boto3
import time
import pandas as pd

s3_output = "s3://patotricks-bucket-example/athena_output/"
database_name = "iris_database"
view_name = "iris_filtered"

athena_client = boto3.client("athena")

query_sql = f"SELECT * FROM {database_name}.{view_name} LIMIT 10"

def run_query(query):
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={"OutputLocation": s3_output}
    )
    return response["QueryExecutionId"]

query_id = run_query(query_sql)
print(f"✅ Executando consulta... Query ID: {query_id}")

def wait_for_query(query_id):
    while True:
        response = athena_client.get_query_execution(QueryExecutionId=query_id)
        status = response["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            return status
        time.sleep(2)

status = wait_for_query(query_id)
if status == "SUCCEEDED":
    print("✅ Consulta finalizada com sucesso!")
else:
    print(f"❌ Erro na consulta. Status: {status}")

def get_query_results(query_id):
    response = athena_client.get_query_results(QueryExecutionId=query_id)

    column_names = [col["Name"] for col in response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]

    rows = []
    for row in response["ResultSet"]["Rows"][1:]:
        rows.append([col["VarCharValue"] if "VarCharValue" in col else None for col in row["Data"]])

    # Criar DataFrame
    df = pd.DataFrame(rows, columns=column_names)

    return df

df_athena = get_query_results(query_id)

print("✅ Resultado da consulta:")
print(df_athena)

df_athena.to_csv("iris_athena_results.csv", index=False)
print("✅ Dados salvos em 'iris_athena_results.csv'")
