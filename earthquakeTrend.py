import pandas as pd
# Bilal Khan 
# CS210
# Summer Project


file_path = '/mnt/data/earthquake_data.csv' #from kagggle

def load_earthquake_data(file_path): #defload_earthquake(file_path);
    df = pd.read_csv(file_path)   #correct
    return df

# csv file type error test
# df=pd.read_csv(file(dath.readf))
#file Path works

earthquake_data = load_earthquake_data(file_path)
print(earthquake_data.head()) # gather step

# print(data)

import sqlite3

def store_data_in_db(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False) #will runn
    conn.close()

# def store data
#conn
# close out

db_name = 'earthquake_data.db'         #full set / 
table_name = 'earthquakes'
store_data_in_db(earthquake_data, db_name, table_name)




#bk test
# dif = dif/iow.read

def clean_data(df):
    
    df = df.dropna(subset=['location'])  
    df['alert'].fillna('unknown', inplace=True)      # true
    df['continent'].fillna('Unknown', inplace=True)   # true
    df['country'].fillna('Unknown', inplace=True)  

    

    
    q1 = df['magnitude'].quantile(0.25)
    q3 = df['magnitude'].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    df = df[(df['magnitude'] >= lower_bound) & (df['magnitude'] <= upper_bound)]

     # df = df[(df['magnitude'] >= lower_bound) & (df['magnitude'] <= right_bound)]
    
    return df


cleaned_data = clean_data(earthquake_data)
print(cleaned_data.head())


def transform_data(df):
    
    df['date_time'] = pd.to_datetime(df['date_time'], format='%d-%m-%Y %H:%M')
    df['day_of_week'] = df['date_time'].dt.day_name()

  
    df['magnitude_scaled'] = (df['magnitude'] - df['magnitude'].min()) / (df['magnitude'].max() - df['magnitude'].min())

    return df

# Example usage
transformed_data = transform_data(cleaned_data)
print(transformed_data.head())


import matplotlib.pyplot as plt

def plot_eda(df):
    plt.figure(figsize=(10, 6))
    plt.hist(df['magnitude'], bins=20, edgecolor='k')
    plt.xlabel('Magnitude')
    plt.ylabel('Frequency')
    plt.title('Distribution of Earthquake Magnitudes')
    plt.show()


plot_eda(transformed_data)


from textblob import TextBlob

def sentiment_analysis(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity




from kafka import KafkaProducer
import json

def kafka_producer_setup(bootstrap_servers):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

def send_data_to_kafka(producer, topic, data):
    for record in data.to_dict(orient='records'):
        producer.send(topic, record)
        producer.flush()


bootstrap_servers = ['localhost:9092']
topic = 'earthquake_topic'
producer = kafka_producer_setup(bootstrap_servers)
send_data_to_kafka(producer, topic, transformed_data)


import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output   # Still not downloaded on system
import plotly.express as px
#app = dash.Dash(Earkuake)
app = dash.Dash(__name__)

app.layout = html.Div([
    dcc.Graph(id='magnitude-plot'),
    dcc.Interval(
        id='interval-component',
        interval=1*1000,  
        n_intervals=0
    )
])

@app.callback(Output('magnitude-plot', 'figure'),
              Input('interval-component', 'n_intervals'))
def update_graph_live(n):
    
    df = load_earthquake_data(file_path)
    df = clean_data(df)
    df = transform_data(df)

    fig = px.histogram(df, x='magnitude', nbins=20)
    return fig

if __name__ == '__main__':      # Shout out youtube again ...
    app.run_server(debug=True)



print(earthquake_data.head())



print(cleaned_data.head()) # works


print(transformed_data.head()) #works



plot_eda(transformed_data) # works



print(social_media_data['sentiment'].head()) # works shoutout youtube



app.run_server(debug=True) # works


# progress

# not done

 #bootstrap_servers = ['localhost:9092']
#topic = 'earthquake_topic'
#producer = kafka_producer_setup(bootstrap_servers)     # DONE
#send_data_to_kafka(producer, topic, transformed_data)

#data wont send, not sure how to work

# - July 1st Need to set up Kafk    #DONE
#June 30 - csv file not working