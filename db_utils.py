import psycopg2
import pandas as pd
import sys
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import math
import numpy as np

load_dotenv()

COLORS_DEF = ['#1D4688','#E31E25','#29524A','#D1AC76','#ABD5CD','#715B64','#BEC5AD'];
CBRSM_COLORS = COLORS_DEF
LW = 1
FNT = 10

# Глобальная переменная для хранения одного соединения
_connection = None

def get_connection():
    """Создает и возвращает единое соединение с БД"""
    global _connection
    if _connection is None or _connection.closed != 0:
        try:
            _connection = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
            )
            # Вывод в стиле fprintf для контроля процесса в консоли/ноутбуке
            # sys.stdout.write("--- INFO: Connected to PostgreSQL successfully ---\n")
        except Exception as e:
            sys.stdout.write("--- ERROR: Unable to connect to database: %s ---\n" % e)
            return None
    return _connection

def sovdb_read(ticker, date):
    """Загружает данные по тикеру и дате"""
    conn = get_connection()
    if conn is None:
        return pd.DataFrame() # Возвращаем пустой DF при ошибке

    query = "SELECT * FROM sovdb_schema.\""+ticker+"\" WHERE \"""Date\""">='"+date.strftime('%Y-%m-%d')+"'"    
    
    try:
        # Используем pandas для удобства создания DataFrame напрямую из SQL
        df = pd.read_sql_query(query, conn, params=(ticker, date))
        df = pd.DataFrame(df).set_index('Date')
        df.index = pd.to_datetime(df.index)    
        df = df.sort_index()

        # Логирование загрузки
        # sys.stdout.write("--- DATA: Loaded %d rows for [%s] on %s ---\n" % (len(df), ticker, date))
        return df
    except Exception as e:
        sys.stdout.write("--- ERROR: Query failed: %s ---\n" % e)
        return pd.DataFrame()

def to_jan(df):
    df = df.dropna()
    month_c = df.index[0].month
    if month_c != 1:
        df = df.iloc[12-month_c+1:,:]
        return df
    else:
        return df

def to_dec(df):
    df = df.dropna()
    month_c = df.index[0].month
    if month_c != 1:
        df = df.iloc[12-month_c:,:]
        return df
    else:
        return df
        
def plot_season(data, freq, titl, is_pop=1, is_norm=0,show_ends=1,RND_TO = 2, diff_colors=0):
    
    if freq=='M':
        freq_n = 12
        xl = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    
    if freq=='Q':
        freq_n = 4
        xl = ['Q1','Q2','Q3','Q4']            
    
    fig, ax = plt.subplots()    
    #если график не нормируем, то рисуем просто как есть
    if is_norm == 0:    
        data = to_jan(data)
        n_y = math.ceil(data.shape[0]/freq_n)
        for i in range(n_y):
            if i==n_y-1:
                temp = data.values[((i+1)-1)*12:]
                if temp.shape[0] == 1:
                    ax.plot(temp, 'xr') 
                else:
                    ax.plot(temp, 'r',linewidth=LW) 
            else:    
                temp = data.values[((i+1)-1)*freq_n:(i+1)*freq_n]
                #st.write(diff_colors)
                if diff_colors:                    
                    ax.plot(temp, linewidth=LW) 
                else:
                    ax.plot(temp, color=CBRSM_COLORS[0], linewidth=LW) 
                    
                if show_ends:
                    ax.text(freq_n-1, float(np.ravel(temp)[-1]), str(data.index[((i+1)-1)*freq_n].year), fontsize=FNT,color=CBRSM_COLORS[0]);    
        plt.xticks([r for r in range(len(xl))], xl)   
        
        if show_ends:
            ax.text(data.index[-1].month-1, float(data.values[-1][0]), str(round(data.values[-1][0],2)), fontsize=FNT,color='r');
            
        #ax.axhline(y=0,color='k', linestyle='-',linewidth=0.4,)          
        ax.set_xticklabels(xl, rotation=90)         
        if freq=='M':
            plt.title(titl+data.index[-1].strftime('%b-%Y'))            
        #plt.title(TAG,fontsize=11,loc='right',color=(0.3,0.3,0.3))  
    #если график нормируем
    else:
        #и данные pop
        if is_pop == 1:
            data = to_jan(data)
            n_y = math.ceil(data.shape[0]/freq_n)
            for i in range(n_y):
                if i==n_y-1:
                    temp = data.values[((i+1)-1)*12:]
                    temp = np.cumprod(1+temp/100)
                    if temp.shape[0] == 1:
                        ax.plot(temp, 'xr') 
                    else:
                        ax.plot(temp, 'r',linewidth=LW) 
                else:    
                    temp = data.values[((i+1)-1)*freq_n:(i+1)*freq_n]
                    temp = np.cumprod(1+temp/100)
                    
                    if data.index[((i+1)-1)*freq_n].year in [2017, 2018, 2019, 2024, 2025]:
                        if diff_colors:                    
                            ax.plot(temp, linewidth=LW) 
                        else:
                            ax.plot(temp, color=CBRSM_COLORS[0], linewidth=LW) 
                            
                        if show_ends:                        
                            if diff_colors:
                                last_line_color = plt.gca().lines[-1].get_color()
                                ax.text(freq_n-1, float(np.ravel(temp)[-1]), str(data.index[((i+1)-1)*freq_n].year), fontsize=FNT,color=last_line_color);    
                            else:
                                ax.text(freq_n-1, float(np.ravel(temp)[-1]), str(data.index[((i+1)-1)*freq_n].year), fontsize=FNT,color=CBRSM_COLORS[0]);    
                                
            plt.xticks([r for r in range(len(xl))], xl)   
            
            if show_ends:
                ax.text(data.index[-1].month-1, float(np.ravel(temp)[-1]), str(round(temp[-1],RND_TO)), fontsize=FNT,color='r');
                
            ax.axhline(y=1,color='k', linestyle='-',linewidth=0.4,)          
            ax.set_xticklabels(xl, rotation=90)         
            if freq=='M':
                plt.title(titl+data.index[-1].strftime('%b-%Y'))            
            #plt.title(TAG,fontsize=11,loc='right',color=(0.3,0.3,0.3))  
        else:
            data = to_dec(data)
            n_y = math.ceil((data.shape[0]-1)/freq_n)
            fig, ax = plt.subplots()
            for i in range(n_y):
                if i==n_y-1:                
                    temp = data.values[((i+1)-1)*freq_n+1:]/data.values[((i+1)-1)*freq_n]
                    if temp.shape[0] == 1:
                        ax.plot(temp, 'xr') 
                    else:                    
                        ax.plot(temp, 'r',linewidth=LW) 
                        if show_ends:
                            #ax.text(temp.shape[0]-1, temp[-1], str(round((temp[-1][0]-1)*100,RND_TO))+"% YTD", fontsize=FNT,color='r');#
                            ax.text(temp.shape[0]-1, float(np.ravel(temp)[-1]), str(round((temp[-1][0]),RND_TO))+"YTD", fontsize=FNT,color='r');#
                else:
    
                    temp = data.values[((i+1)-1)*12+1:(i+1)*12+1]/data.values[((i+1)-1)*12]                
                    ax.plot(temp, color=CBRSM_COLORS[0],linewidth=LW) 
                    if show_ends:
                        ax.text(freq_n-1, float(np.ravel(temp)[-1]), str(data.index[((i+1)-1)*12].year), fontsize=FNT,color=CBRSM_COLORS[0]);
    
            plt.xticks([r for r in range(len(xl))], xl)   
            ax.set_xticklabels(xl, rotation=90) 
            if freq=='M':
                plt.title(titl+data.index[-1].strftime('%b-%Y'))
            #plt.title(TAG,fontsize=11,loc='right',color=(0.3,0.3,0.3))           
    plt.show()
    return fig, ax    
        