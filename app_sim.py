

# importar dependencias
import streamlit as st # web development
import numpy as np # np mean, np random 
import pandas as pd # read csv, df manipulation
import time # to simulate a real time data, time loop 
import plotly.express as px # interactive charts
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, time, timedelta, date
from time import sleep
from pymysql import connect
import os
import dotenv
from textwrap import dedent
from millify import millify, prettify


### DEFINICIONES DE PAGINA ###

st.set_page_config(
    page_title = 'Easy-Chat-Services',
    # page_icon = '🏭',
    layout = 'wide'
)

### DEFINICION DE LA APP ###

# título del dashboard

st.markdown("# Easy Chat Services LLC - Call monitoring app")

# columnas para simulación de la app
sim_col1, separador_sim, sim_col2 = st.columns([15, 2, 30])

with sim_col1:
    st.markdown('#### Simulation')
    sim = st.toggle("Sim on / off",
                    value=True)
    if sim:

        st.write('App running on sim mode')
        
        fecha = st.date_input("Date to start simulation", 
                              date(2024, 8, 8),
                              format='YYYY-MM-DD',
                              )
        # st.write(fecha)
        # print(fecha.strftime('%Y-%m-%d %H:%M:%S'))

        frecuencia = st.radio(
            "Wait time between iterations:",
            ["1 sec", "5 sec", "15 sec"],
            index=1,
            )

        if frecuencia == '1 sec':
            freq_sim = 1
        elif frecuencia == '5 sec':
            freq_sim = 5
        else:
            freq_sim = 15
        st.write(f"Wait time is {freq_sim} seconds")

# línea divisora 
st.divider()


# columnas para configuración de la app
setup_col1, separarador_setup, setup_col2 = st.columns([15, 2, 30])

with setup_col1:
    st.markdown('#### Set up')
    
    setup_main = st.toggle("Setup on / off",
                           value=False,
                           )

    if setup_main:
        horario = st.slider(
            "Time frame:", 
            value=(time(6, 00), time(20, 00)),
            step=timedelta(minutes=30),
            )

        frecuencia = st.radio(
            "Frequency:",
            ["5 min", "15 min", "30 min"],
            )

        if frecuencia == '5 min':
            st.write("app will run queries every 5 min")
            freq = 5
        elif frecuencia == '15 min':
            st.write("app will run queries every 15 min")
            freq = 15
        else:
            st.write("app will run queries every 30 min")
            freq = 30
    else:
        freq = 5
        horario = (time(0,0,0), time(23,59,00))


with setup_col2:
    st.markdown("""
#### Instructions:
- Choose the parameters for app operation
    - Toggle Setup on/off
    - Time frame: hours in which the app is running
    - Frequency: interval to check the call cengter data base

                """)

# línea divisora 
st.divider()

# diseño de la página
placeholder_body = st.empty()
placeholder_footer = st.empty()

with placeholder_footer.container():
    st.divider()
    st.markdown('Developed by HEPTAGON')
    st.markdown('Carlos Gorricho')
    st.markdown('cgorricho@heptagongroup.co')
    st.markdown('cel COL +57 314 771 0660')
    st.markdown('cel USA +1 (305) 381-1335')

# *** OBTENER DATOS ***

# carga credenciales de archivo .env
dotenv.load_dotenv('.env')

# lista de marcadores
url_marcadores = ['83.138.55.125',
              '193.219.97.92',
              '193.219.97.90',
              '83.138.55.115',
              '94.177.9.69',
              '83.138.55.163',
              ]

ind_marcadores = [1, 2, 3, 4, 6, 7]

url_backup = '86.106.183.70'

database_backup = ['marcador1',
                   'marcador2',
                   'marcador3',
                   'marcador4',
                   'marcador6',
                   'marcador7']

# función para concectarse a servidor backup
def connect_to_db_backup():
    bd = {}
    cursores = {}

    for dbase in database_backup:
        try:    
            # crea la conexión
            bd[f'{dbase}'] = connect(host = url_backup,
                                            user=os.getenv('EASYCHAT_SQL_USER'),
                                            passwd=os.getenv('EASYCHAT_SQL_PSWD'),
                                            database=dbase,
                                            )

            # crea el cursor
            cursores[f'cursor_{dbase}'] = bd[f'{dbase}'].cursor()
            print(f'Conexión exitosa a base de datos {dbase} con ip {url_backup}')
        except:
            print(f'Error conectando a base de datos {dbase} con ip {url_backup}')
            continue
    return bd, cursores


# crea una función para devolver cada dataframe
def get_df(bd, bd_name, query):
    read_ok = False
    while not read_ok:
        try:
            df_temp = pd.read_sql(query, bd)
            read_ok = True
        except:
            sleep(30)
            bd, _ = connect_to_db_backup()
    df_temp['bd'] = bd_name
    df_temp.starttime = pd.to_datetime(df_temp.starttime)
    df_temp.calledstation = df_temp.calledstation.astype('category')
    df_temp.real_sessiontime = df_temp.real_sessiontime.astype('int')
    df_temp.bd = df_temp.bd.astype('category')
    return df_temp


# obtiene datos de los marcadores
def fetch_data(fecha_inicial, fecha):
    bd, _ = connect_to_db_backup()
    query = dedent(f"""
        SELECT starttime, calledstation, real_sessiontime
        FROM pkg_cdr
        WHERE starttime BETWEEN '{fecha_inicial}' AND '{fecha}';
        """)
    print(query)
    df_list = []
    for dbase in database_backup:
        query_ok = False
        conn_ok = False
        print(f'Consultando {dbase}...')
        # while not query_ok:
        try:
            df_list.append(get_df(bd[dbase], dbase, query))
            query_ok = True
            print(f'Consulta {dbase} OK')
        except Exception as e:
            print(f'Consulta {dbase} con ERROR {e}')
                # while not conn_ok:
                #     try:
                #         bd, _ = connect_to_db_backup()
                #         conn_ok = True
                #         print(f'Nueva conexión bd OK')
                #     except:
                #         print(f'Nueva conexión bd con ERROR')
                #         sleep(5)
    df = pd.concat(df_list, axis=0)
    print(df.info())
    print(df.head())
    df['weekday'] = df.starttime.dt.weekday
    df['hour'] = df.starttime.dt.hour
    df = df.set_index('starttime')
    df = df.sort_index()
    for dbase in database_backup:
        bd[dbase].close()
    return df


# agrega datos a los marcadores para bloquear números de destino
def add_data(df_flat):
    # establece los conectores a los servidores mysql
    bd, cursores = connect_to_db_backup()

    # crea cadena de tuplas
    block_numbers = ''
    ind = 0
    index_lm = df_flat.index
    num_block = len(index_lm) # solo para simulación

    while ind < num_block:
        block_numbers += "("
        block_numbers += index_lm[ind]
        block_numbers += ","
        block_numbers += f"'OPERACION AGOSTO 2024 Número agregado desde Python el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"
        if ind < num_block - 1:
            block_numbers += "),\n"
        else:
            block_numbers += ")"
        ind += 1

    # cosntruye la consulta
    add_query = dedent(f"""INSERT INTO `pkg_campaign_restrict_phone` (number, description) VALUES {block_numbers};""")

    # ejecuta consulta en todos los marcadores
    for key in cursores:
        cursores[key].execute(add_query)
        print(f'Ejectando consulta en {key} - Agregadas {cursores[key].rowcount} filas')

    # confirma la consulta en todos los marcadores
    for base in bd:
        bd[base].commit()
        print(f'Condirmando cambios en {base}')       

    # cierra las connecciones a los servidores mysql
    for dbase in database_backup:
        bd[dbase].close()
    
    return cursores[key].rowcount


# función para el cálculo del ratio "línea plana" en la agrupación por número de destino
def ratio(df_func, last_n=2):
    df_temp = df_func.tail(last_n)
    ratio = df_temp['real_sessiontime'].std() / df_temp['real_sessiontime'].mean()
    return ratio

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
    df_dest = df_dest.sort_values(by='num_llamadas', ascending=False)

    # calcula ratio "línea plana" para cada número de destino
    df_dest_ratio = df.groupby('calledstation').apply(ratio).to_frame().rename(columns={0: 'flat_ratio'})

    # unión de dataframe por número de destino
    df_dest = df_dest.merge(df_dest_ratio, left_index=True, right_index=True)
    
    # crea dataframe con números de destino que tienen línea plana
    umbral = 0.005
    df_dest_flat = df_dest[df_dest.flat_ratio <= umbral].sort_values(by='real_sessiontime', ascending=False)

    return df_dest, df_dest_flat


# define main function
def main_func(fecha_inicio, fecha): 
    
    #captura variable global
    global stats_dict, dur_iter

    # presenta la información
    with placeholder_body.container():
        
        # calcula la fecha menor de la ventana de consulta a la BD
        fecha_inicial = max(fecha_inicio, (fecha - timedelta(days=15)))
        
        st.subheader(f'Fetching data between :blue[{fecha_inicial}] and :blue[{fecha}]')

        # cronómetro
        start_iter = datetime.now()
        
        # obtiene datos
        df = fetch_data(fecha_inicial, fecha)

        # transforma datos para crear hoja de vida de números de destino
        if not df.empty:
            df_dest, df_dest_flat = destination_df(df)
        else:
            df_dest = pd.DataFrame()
            df_dest_flat = pd.DataFrame()

        print(f'df dest: {len(df_dest)}')
        print(f'df dest flat: {len(df_dest_flat)}')

        # bloquea números con flat line
        if not df_dest_flat.empty:
            num_bloq = add_data(df_dest_flat)
        else:
            num_bloq = 0

        # define indicadores
        total_calls = len(df)
        dest_nums = len(df_dest)
        dest_nums_flat = len(df_dest_flat)
        if not df_dest.empty:
            avg_dur = df_dest.dur_prom.mean()
        else:
            avg_dur = 0
        if not df_dest_flat.empty:
            avg_dur_flat = df_dest_flat.dur_prom.mean()
        else:
            avg_dur_flat = 0
        nums_bloq = num_bloq
        
        
        # primera fila de indicadores
        kpi11, kpi12, kpi13, kpi14, kpi15 = st.columns(5)
        kpi11.metric(label="Total calls", value=millify(total_calls, precision=2, prefixes=[' K', ' MM']), delta=total_calls - stats_dict['total_calls'])
        kpi12.metric(label="Dest numbers", value=millify(dest_nums, precision=2, prefixes=[' K', ' MM']), delta=dest_nums - stats_dict['dest_nums'])
        kpi13.metric(label="Dest numbers flat line", value=millify(dest_nums_flat, precision=2, prefixes=[' K', ' MM']), delta=dest_nums_flat - stats_dict['dest_nums_flat'])
        kpi14.metric(label="Avg call duration", value=millify(avg_dur, precision=2, prefixes=[' K', ' MM']), delta=round(avg_dur - stats_dict['avg_dur'], 2))
        kpi15.metric(label="Avg call duration flat line", value=millify(avg_dur_flat, precision=2, prefixes=[' K', ' MM']), delta=round(avg_dur_flat - stats_dict['avg_dur_flat'], 2))

        
        # segunda fila de indicadores
        kpi21, kpi22, kpi23, kpi24, kpi25 = st.columns(5)
        kpi23.metric(label="Blocked dest numbers", value=millify(num_bloq, prefixes=[' K', ' MM']), delta=nums_bloq - stats_dict['nums_bloq'])

        # asigna indicadores a dict
        stats_dict['total_calls'] = len(df)
        stats_dict['dest_nums'] = len(df_dest)
        stats_dict['dest_nums_flat'] = len(df_dest_flat)
        if not df_dest.empty:
            stats_dict['avg_dur'] = df_dest.dur_prom.mean()
        else:
            stats_dict['avg_dur'] = 0
        if not df_dest_flat.empty:
            stats_dict['avg_dur_flat'] = df_dest_flat.dur_prom.mean()
        else:
            stats_dict['avg_dur_flat'] = 0
        stats_dict['nums_bloq'] = num_bloq
        
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
            st.write(df.tail(20))
        with datos_col2:
            st.markdown("#### Top 20 destination numbers flat line")
            st.write(df_dest_flat.head(20))
            st.write(f'Flat_ratio = std / mean of last 2 calls')

        # para cronómetro e imprime tiempo de cada ciclo
        end_iter =datetime.now()
        elapsed = end_iter - start_iter
        dur_iter.append(elapsed.microseconds)
        st.write(f'Iteration {cont} took {elapsed}')

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

      

### MAIN BODY OF SCRIPT ###       

# definición de variables iniciales
cont = 1
sim_date = datetime(fecha.year, fecha.month, fecha.day, 12, 0, 0)
init_date = datetime(2024, 8, 6, 12, 0, 0)
stop_date = datetime(2024, 8, 31)
iter_sim = int((stop_date - datetime(fecha.year, fecha.month, fecha.day)).days / freq * 60 * 24)

stats_dict = {
    'total_calls': 0,
    'dest_nums': 0,
    'dest_nums_flat': 0,
    'avg_dur': 0,
    'avg_dur_flat': 0,
    'nums_bloq': 0
    }

dur_iter = [0]


for iter in range(iter_sim):

    main_func(init_date, sim_date)

    if sim_date.hour <= 21 and sim_date.hour >= 7:
        sim_date += timedelta(minutes=freq)
    elif sim_date.hour > 21 and sim_date.hour < 24:
        sim_date += timedelta(hours=(24 - sim_date.hour + 1))
        sim_date = datetime(sim_date.year, sim_date.month, sim_date.day, 7, 0, 0)
    else:
        sim_date = datetime(sim_date.year, sim_date.month, sim_date.day, 7, 0, 0)
        
    cont += 1








