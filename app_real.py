

# importar dependencias
import streamlit as st # web development
import numpy as np # np mean, np random 
import pandas as pd # read csv, df manipulation
import time # to simulate a real time data, time loop 
import plotly.express as px # interactive charts
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, time, timedelta, date, timezone
from time import sleep
from pymysql import connect
import os
import dotenv
from textwrap import dedent
from millify import millify, prettify
from functools import reduce
import warnings
warnings.filterwarnings('ignore')


### DEFINICIONES DE PAGINA ###

st.set_page_config(
    page_title = 'Easy-Chat-Services',
    # page_icon = '🏭',
    layout = 'wide'
)

### DEFINICION DE LA APP ###

# título del dashboard

st.markdown("# Easy Chat Services LLC - Call monitoring app")


# elemento expansión para parámetros de configuración de app
with st.sidebar:

    st.markdown('# Set up')
    
    # Monitoring start date
    start_date = st.date_input("Monitoring start date", datetime(2024, 10, 15))
            
    
    # Operating hours
    horario = st.slider(
        "Monitoring hours", 
        min_value=time(00, 00),
        max_value=time(23, 59),
        value=(time(7, 00), time(22, 00)),
        step=timedelta(minutes=30),
        )
    
    # frequency for fetching data 
    frecuencia = st.radio(
        "Fetch data every",
        ["1 min","5 min", "15 min", "30 min"],
        index=1,
        )

    if frecuencia == '5 min':
        # st.write("app will run queries every 5 min")
        freq = 5
    elif frecuencia == '1 min':
        # st.write("app will run queries every 15 min")
        freq = 1
    elif frecuencia == '15 min':
        # st.write("app will run queries every 15 min")
        freq = 15
    else:
        # st.write("app will run queries every 30 min")
        freq = 30

    # time window to fetch data
    days_window = st.radio(
        "Fetch data for last",
        ["7 days", "15 days", "30 days", "365 days"],
        index=3,
        )

    if days_window == '7 days':
        # st.write("app will run queries every 5 min")
        days = 7
    elif days_window == '15 days':
        # st.write("app will run queries every 15 min")
        days = 15
    elif days_window == '30 days':
        # st.write("app will run queries every 30 min")
        days = 30
    else:
        days = 365

    # umbral para flat ratio
    umbral = st.select_slider(
        "Flat-ratio threshhold", 
        options=[0.0015,
                 0.0025,
                 0.005,
                 0.0075,
                 0.01,
                 ],
        # format_func='%4f'
        )
    
    
# línea divisora 
st.divider()

# diseño de la página
placeholder_body = st.empty()
placeholder_footer = st.empty()

with placeholder_footer.container():
    st.divider()
    st.markdown('Carlos Gorricho AI')
    st.markdown('cgorricho@heptagongroup.co')
    st.markdown('cel COL +57 314 771 0660')
    st.markdown('cel USA +1 (305) 381-1335')

# *** OBTENER DATOS ***

# carga credenciales de archivo .env
dotenv.load_dotenv('.env')

# lista de marcadores
url_marcadores = ['185.194.217.4',
                  '155.133.27.24']

ind_marcadores = [1,
                  2]

pwd_marcadores = [os.getenv('EASYCHAT_SQL_PSWD_MARCADOR1'),
                  os.getenv('EASYCHAT_SQL_PSWD_MARCADOR2')]


# función para concectarse a servidor productivo
def connect_to_db():
    
    bd = {}
    cursores = {}

    for i, ind in enumerate(ind_marcadores):
        # crea la conexión
        try:    
            bd[f'marcador_{ind}'] = connect(host = url_marcadores[i],
                                            user=os.getenv('EASYCHAT_SQL_USER'),
                                            passwd=pwd_marcadores[i],
                                            database='mbilling',
                                            )

            # crea el cursor
            cursores[f'cursor_marcador_{ind}'] = bd[f'marcador_{ind}'].cursor()
            print(f'Función connect_to_db: Conexión exitosa a marcador_{ind} con ip {url_marcadores[i]}')
        except Exception as e:
            print(f'Función connect_to_db: Error {e} conectando a marcador_{ind} con ip {url_marcadores[i]}')
            continue
    return bd, cursores


# crea una función para devolver cada dataframe con la información de llamadas
def get_df(bd, bd_name, query):
    read_ok = False
    while not read_ok:
        try:
            df_temp = pd.read_sql(query, bd)
            read_ok = True
        except Exception as e:
            print(f'Función get_df: Error {e} intentando leer datas en DataFrame. Esperando por 5 segundos')
            sleep(5)
            bd, _ = connect_to_db()
    df_temp['bd'] = bd_name
    df_temp.starttime = pd.to_datetime(df_temp.starttime)
    df_temp.calledstation = df_temp.calledstation.astype('category')
    df_temp.real_sessiontime = df_temp.real_sessiontime.astype('int')
    df_temp.bd = df_temp.bd.astype('category')
    return df_temp


# obtiene datos de llamadas de los marcadores
def fetch_call_data(fecha_inicial, fecha):
    bd, _ = connect_to_db()
    query = dedent(f"""
        SELECT starttime, calledstation, real_sessiontime
        FROM pkg_cdr
        WHERE starttime BETWEEN '{fecha_inicial}' AND '{fecha}';
        """)
    df_list = []
    for ind in ind_marcadores:
        query_ok = False
        print(f'Función fetch_call_data: Consultando marcador_{ind}...')
        # while not query_ok:
        try:
            df_list.append(get_df(bd[f'marcador_{ind}'], f'marcador_{ind}', query))
            query_ok = True
            print(f'Función fetch_call_data: Consulta marcador_{ind} OK')
        except Exception as e:
            print(f'Función fetch_call_data: Consulta marcador_{ind} con ERROR {e}')
    df = pd.concat(df_list, axis=0)
    print(f'Función fetch_call_data: \n{df.info()}')
    print(f'Función fetch_call_data: \n{df.head()}')
    df['weekday'] = df.starttime.dt.weekday
    df['hour'] = df.starttime.dt.hour
    df = df.set_index('starttime')
    df = df.sort_index()
    for ind in ind_marcadores:
        bd[f'marcador_{ind}'].close()
    return df


# obtiene datos números bloqueados de los marcadores
def fetch_block_data():
    bd, _ = connect_to_db()
    query_block = dedent(f"""
        SELECT number
        FROM pkg_campaign_restrict_phone;
        """)
    df_list = []
    for ind in ind_marcadores:
        query_ok = False
        print(f'Consultando marcador_{ind}...')
        # while not query_ok:
        try:
            df_list.append(pd.read_sql(query_block, bd[f'marcador_{ind}']))
            query_ok = True
            print(f'Consulta marcador_{ind} OK')
        except Exception as e:
            print(f'Consulta marcador_{ind} con ERROR {e}')
    df = reduce(lambda left, right: pd.merge(left, right, on='number', how='inner'), df_list)
    for ind in ind_marcadores:
        bd[f'marcador_{ind}'].close()
    return df


# agrega datos a los marcadores para bloquear números de destino
def block_dest_nums(df_flat, fecha):
    # establece los conectores a los servidores mysql
    bd, cursores = connect_to_db()

    
    
    
    
    
    # crea lista de tuplas *** DEBERIA CREARLA SONRE EL DF FINAL PARA BLOQUEO
    index_fl = df_flat.index
    
    for ind in index_fl:
        numbers_data = []
        numbers_data.append((ind, f"'{fecha.strftime('%Y-%m-%d %H:%M:%S')}'"))
    
    # cosntruye la consulta
    insert_query = dedent(f"""INSERT INTO `pkg_campaign_restrict_phone` (number, description) 
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE 
                            description = VALUES(description);""")

    # ejecuta consulta en todos los marcadores
    for key in cursores:
        cursores[key].executemany(insert_query, numbers_data)
        print(f'Ejectando consulta en {key} - Agregadas {cursores[key].rowcount} filas')

    # confirma la consulta en todos los marcadores
    for base in bd:
        bd[base].commit()
        print(f'Confirmando cambios en {base}')       

    # cierra las connecciones a los servidores mysql
    for ind in ind_marcadores:
        bd[f'marcador_{ind}'].close()
    
    return cursores[key].rowcount


# función para el cálculo del ratio "línea plana" en la agrupación por número de destino
def ratio(df_func, last_n=2):
    df_temp = df_func.tail(last_n)
    ratio = df_temp['real_sessiontime'].std() / df_temp['real_sessiontime'].mean()
    return ratio

# función para el cálculo de Desv Standard del ratio "línea plana" en la agrupación por número de destino
def std_dev(df_func, last_n=2):
    return df_func['real_sessiontime'].tail(last_n).std()

# función para el cálculo de Promedio del ratio "línea plana" en la agrupación por número de destino
def prom(df_func, last_n=2):
    return df_func['real_sessiontime'].tail(last_n).mean()


# crea destination dataframe
def destination_df(df):
    
    # número de llamadas por número de destino
    df_dest_calls = df.calledstation.value_counts().to_frame().rename(columns={'count': 'num_llamadas'})
    
    # suma total de minutos por número de destino
    df_dest = df.groupby(['calledstation']).real_sessiontime.sum().sort_values(ascending=False).to_frame()

    # unión de dataframe por número de destino
    df_dest = df_dest.join(df_dest_calls, on='calledstation')

    # cálculo de duración promedio de llamada
    df_dest['dur_prom'] = df_dest.real_sessiontime / df_dest.num_llamadas

    # ordena por número total de llamadas
    df_dest = df_dest.sort_values(by=['num_llamadas','real_sessiontime'], ascending=[False, False])

    # calcula ratio "línea plana" para cada número de destino
    df_dest_ratio = pd.DataFrame()
    df_dest_ratio[f'std_lst_2'] = df.groupby('calledstation').apply(std_dev)
    df_dest_ratio[f'mean_lst_2'] = df.groupby('calledstation').apply(prom)
    df_dest_ratio['flat_ratio'] = df_dest_ratio['std_lst_2'] / df_dest_ratio['mean_lst_2']

    # unión de dataframe por número de destino
    df_dest = df_dest.merge(df_dest_ratio, left_index=True, right_index=True)
    
    # crea dataframe con números de destino que tienen línea plana
    df_dest_flat = df_dest[df_dest.flat_ratio <= umbral].sort_values(by=['num_llamadas','real_sessiontime'], ascending=[False, False])

    return df_dest, df_dest_flat


# define main function
def main():

    # definición de variables iniciales
    now = datetime.now(timezone.utc) 
    fecha = now.astimezone(timezone(timedelta(hours=-5))).replace(tzinfo=None)
    fecha_inicio = datetime(start_date.year, start_date.month, start_date.day)
    
    # inicializa dataframe para kpis a lo largo de las actualizaciones de la app
    if 'df_kpi' not in st.session_state:
        st.session_state.df_kpi = pd.DataFrame(columns=['unix_ts', 
                                                        'timestamp',
                                                        'total_calls',
                                                        'marcadores', 
                                                        'dest_nums', 
                                                        'dest_nums_flat', 
                                                        'avg_dur', 
                                                        'avg_dur_flat',
                                                        'nums_bloq',
                                                        'dur_iter'])
    
    
    # presenta la información
    with placeholder_body.container():

        # calcula la fecha menor de la ventana de consulta a la BD
        fecha_inicial = max(fecha_inicio, (fecha - timedelta(days=days)))
        fecha_inicial_display = datetime(fecha_inicial.year, fecha_inicial.month, fecha_inicial.day, fecha_inicial.hour, fecha_inicial.minute, round(fecha_inicial.second))

        # calcula la fecha para imprimir en pantalla de web app
        fecha_display = datetime(fecha.year, fecha.month, fecha.day, fecha.hour, fecha.minute, round(fecha.second))

        # registra inicio iteración
        print(f'\n***** Inicia nueva iteración en {fecha_display} *****')

        # imprime la ventana de tiempo para la cual está obteniendo información
        st.markdown(f'## Fetching data for a :blue[{days}-day] window, between')
        st.markdown(f'### Start: :blue[{fecha_inicial_display}]')
        st.markdown(f'### End: :blue[{fecha_display}]')

        # cronómetro
        start_iter = datetime.now()
        
        # obtiene datos
        df = fetch_call_data(fecha_inicial, fecha)
        df.to_csv('df_llamadas.csv')

        # transforma datos para crear hoja de vida de números de destino
        if not df.empty:
            df_dest, df_dest_flat = destination_df(df)
        else:
            df_dest = pd.DataFrame()
            df_dest_flat = pd.DataFrame()

        print(f'df dest: {len(df_dest)}')
        print(f'df dest flat: {len(df_dest_flat)}')
        df_dest.to_csv('df_dest.csv')
        df_dest_flat.to_csv('df_dest_flat.csv')

        # bloquea números con flat line
        if not df_dest_flat.empty:
            nums_bloq = block_dest_nums(df_dest_flat, fecha)
        else:
            nums_bloq = 0

        # define indicadores
        df_temp = pd.DataFrame(dict(unix_ts = fecha.timestamp(),
                                    timestamp = fecha,
                                    total_calls = len(df),
                                    marcadores = df.bd.nunique(),
                                    dest_nums = len(df_dest),
                                    dest_nums_flat = len(df_dest_flat),
                                    avg_dur = df_dest.dur_prom.mean() if not df_dest.empty else 0, 
                                    avg_dur_flat = df_dest_flat.dur_prom.mean() if not df_dest_flat.empty else 0,
                                    nums_bloq = nums_bloq),
                                index=[0]
                                )

        # create temp pandas df
        st.session_state.df_kpi = pd.concat([st.session_state.df_kpi, df_temp], ignore_index=True)
        st.session_state.df_kpi.to_csv('df_kpi.csv')

        # calculate delta kpi
        min_unix_ts = st.session_state.df_kpi.unix_ts.iat[0]
        curr_unix_ts = st.session_state.df_kpi.unix_ts.iat[-1]
        if min_unix_ts == curr_unix_ts:
            total_call_delta = st.session_state.df_kpi.iloc[-1].loc['total_calls']
            dest_nums_delta = st.session_state.df_kpi.iloc[-1].loc['dest_nums']
            dest_nums_flat_delta = st.session_state.df_kpi.iloc[-1].loc['dest_nums_flat']
            avg_dur_delta = st.session_state.df_kpi.iloc[-1].loc['avg_dur']
            avg_dur_flat_delta = st.session_state.df_kpi.iloc[-1].loc['avg_dur_flat']
            nums_bloq_delta = st.session_state.df_kpi.iloc[-1].loc['nums_bloq']
        else:
            total_call_delta = st.session_state.df_kpi.iloc[-1].loc['total_calls'] - st.session_state.df_kpi.iloc[-2].loc['total_calls']
            dest_nums_delta = st.session_state.df_kpi.iloc[-1].loc['dest_nums'] - st.session_state.df_kpi.iloc[-2].loc['dest_nums']
            dest_nums_flat_delta = st.session_state.df_kpi.iloc[-1].loc['dest_nums_flat'] - st.session_state.df_kpi.iloc[-1].loc['dest_nums_flat']
            avg_dur_delta = st.session_state.df_kpi.iloc[-1].loc['avg_dur'] - st.session_state.df_kpi.iloc[-2].loc['avg_dur']
            avg_dur_flat_delta = st.session_state.df_kpi.iloc[-1].loc['avg_dur_flat'] - st.session_state.df_kpi.iloc[-1].loc['avg_dur_flat']
            nums_bloq_delta = st.session_state.df_kpi.iloc[-1].loc['nums_bloq'] - st.session_state.df_kpi.iloc[-1].loc['nums_bloq']
        
        
        # Crea tabulaciones
        tab1, tab2, tab3, tab4, tab5 = st.tabs(['Main', 'Blocked', 'Oth1', 'Oth2', 'Oth3'])

        # configura espacio de tab1: Página principal
        with tab1:
        
            # primera fila de indicadores
            kpi11, kpi12, kpi13, kpi14, kpi15 = st.columns(5)
            kpi11.metric(label="Total calls", value=millify(st.session_state.df_kpi.iloc[-1].loc['total_calls'], precision=2, prefixes=[' K', ' MM']), delta=total_call_delta)
            kpi12.metric(label="Dest numbers", value=millify(st.session_state.df_kpi.iloc[-1].loc['dest_nums'], precision=2, prefixes=[' K', ' MM']), delta=dest_nums_delta)
            kpi13.metric(label="Dest numbers flat line", value=millify(st.session_state.df_kpi.iloc[-1].loc['dest_nums_flat'], precision=2, prefixes=[' K', ' MM']), delta=dest_nums_flat_delta)
            kpi14.metric(label="Avg call duration", value=millify(st.session_state.df_kpi.iloc[-1].loc['avg_dur'], precision=2, prefixes=[' K', ' MM']), delta=round(avg_dur_delta, 2))
            kpi15.metric(label="Avg call duration flat line", value=millify(st.session_state.df_kpi.iloc[-1].loc['avg_dur_flat'], precision=2, prefixes=[' K', ' MM']), delta=round(avg_dur_flat_delta, 2))

            
            # segunda fila de indicadores
            kpi21, kpi22, kpi23, kpi24, kpi25 = st.columns(5)
            kpi21.metric(label="Active servers", value=st.session_state.df_kpi.iloc[-1].loc['marcadores'])
            kpi23.metric(label="Blocked dest numbers", value=millify(st.session_state.df_kpi.iloc[-1].loc['nums_bloq'], precision=2, prefixes=[' K', ' MM']), delta=nums_bloq_delta)


            # primera fila de gráficas
            fig_col1, fig_col2 = st.columns(2)
            with fig_col1:
                st.markdown("#### Calls by hour of day")
                fig = px.bar(df.groupby('hour').count(), 
                            x=df.groupby('hour').count().index, 
                            y='calledstation',
                            labels={
                                'x': 'Hour of day',
                                'calledstation': 'number of calls',
                            })
                fig.update_layout(xaxis = dict(
                                    tickmode = 'linear',
                                    tick0 = 7,
                                    dtick = 1)
                                )
                st.plotly_chart(fig, use_container_width=True)
                st.write('')
            with fig_col2:
                st.markdown("#### Calls by day of week")
                fig2 = px.bar(df.groupby('weekday').count(), 
                            x=df.groupby('weekday').count().index, 
                            y='calledstation',
                            labels={
                                'x': 'Day of week (Mon=0, Sun=6)',
                                'calledstation': 'number of calls',
                            })
                fig2.update_layout(xaxis = dict(
                                    tickmode = 'linear',
                                    tick0 = 1,
                                    dtick = 1)
                                )
                st.plotly_chart(fig2, use_container_width=True)
        
            # segunda fila de gráficas
            fig_col21, fig_col22 = st.columns(2)
            
            # línea divisora 
            st.divider()
            
            # zona de datos
            datos_col1, datos_col2 = st.columns(2)
            with datos_col1:
                st.markdown("#### Last 20 calls")
                st.dataframe(df.tail(20),
                             use_container_width=True)
            with datos_col2:
                st.markdown("#### Top 20 dest nums flat line")
                st.dataframe(df_dest_flat.head(20).style.format(subset=['dur_prom', 'std_lst_2', 'mean_lst_2'], formatter="{:.2f}"),
                             use_container_width=True)
                st.write(f'Flat_ratio = std / mean of last 2 call duration')

            # para cronómetro e imprime tiempo de cada ciclo
            end_iter =datetime.now()
            elapsed = end_iter - start_iter
            # dur_iter.append(elapsed.microseconds)
            hour_elapsed = round(float(str(elapsed).split(':')[0]))
            min_elapsed = round(float(str(elapsed).split(':')[1]))
            sec_elapsed = round(float(str(elapsed).split(':')[2]), 2)
            st.write(f'Iteration took {min_elapsed} min and {sec_elapsed} sec')

            # tercera fila de gráficas
            fig_col31, fig_col32, fig_col33, fig_col34 = st.columns(4)
            # with fig_col31:
            #     if len(dur_iter) > 0:
            #         st.markdown("#### Calls by day of week")
            #         x_list = [i for i in range(len(dur_iter))]
            #         df_iter = pd.DataFrame({'x': x_list, 'y': dur_iter})
            #         fig5 = px.line(df_iter, 
            #                     x='x', 
            #                     y='y',
            #                     )
            #         st.plotly_chart(fig5, use_container_width=True)

    
    # define varible de control de iteraciones
    if horario[0] <= time(fecha.hour, 00) < horario[1]:
        # Auto-refresh every freq minutes (300_000 ms)
        refresh = freq * 60 * 1000
        count = st_autorefresh(interval=refresh, key="data_refresh")



# Run the app
if __name__ == "__main__":
    main()
