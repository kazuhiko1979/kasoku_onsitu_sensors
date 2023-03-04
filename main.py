import streamlit as st
import matplotlib.pyplot as plt
import sqlite3
import boto3
import pandas as pd
import datetime
import altair as alt
import json
import datetime as dt
import smtplib
from email.mime.text import MIMEText
import login
import requests
import paramiko
import os

from matplotlib.backends.backend_pdf import PdfPages
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Meiji Sensor Dashboard", layout="wide")

if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None


if __name__ == "__main__":

    # ログイン認証に成功すれば処理切り替え
    if st.session_state['authentication_status']:

        if st.sidebar.button("ログアウト"):
            st.session_state['authentication_status'] = None
            st.experimental_rerun()

        # 自動更新　例：1分毎　interval= 1 * 60 *1000 
        st_autorefresh(interval=0.5 * 60 * 1000, key="dataframerefresh")    

        # DynamoDBリソースを用いたセッションの初期化、およびアクセスキーとシークレットアクセスキーの指定
        dynamodb = boto3.resource('dynamodb',
                                region_name='ap-northeast-1',
                                aws_access_key_id='AKIAYKSH6JZNKIFSHJYE',
                                aws_secret_access_key='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')

        # DynamoDB テーブルの取得
        table = dynamodb.Table('dynamo_test')

        # テーブル抽出
        result = table.scan()

        data = result['Items']

        # JSON形式で取得したデータをPythonデータフレーム変換準備
        for i in data:
            if "X" in i['payload']:
                if i['payload']['X']:
                    i['payload']['X'] = float(i['payload']['X']) 
                if i['payload']['Y']:
                    i['payload']['Y'] = float(i['payload']['Y'])
                if i['payload']['Z']:
                    i['payload']['Z'] = float(i['payload']['Z'])
            elif "temperature" in i['payload']:
                i['payload']['temperature'] = float(i['payload']['temperature'])
                i['payload']['humidity'] = float(i['payload']['humidity'])
                
                
                while 'LastEvaluateKey' in result:
                    result = table.scan(ExclusiveStartKey=result['LastEvalutedKey'])
                    data.extend(result['Items'])

        # JSONからデータフレームに変換
        df = pd.json_normalize(data)

        # 列名変更
        df = df.rename(columns={'timestamp':'Date', 'payload.temperature': 'temp', 'payload.humidity': 'humid', 'payload.power': 'power', 'payload.Serial_ID': 'sid', 'payload.X': 'x', 'payload.Y': 'y', 'payload.Z':'z'})

        # タイムスタンプ13桁->10桁変換：時刻変換
        df['Date'] = df['Date'].apply(lambda d: datetime.datetime.fromtimestamp(int(d)/1000).strftime('%Y-%m-%d %H:%M:%S'))

        # datetimeオブジェクトをdatetime型に変換
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')

        # users & sensors DB 指定
        # users_sensors_db = '/home/ec2-user/meiji_aws_product/db/users_sensors.db'
        users_sensors_db = 'C:\\meiji_aws_product\\db\\users_sensors.db'
        
        # 登録センサー名抽出
        with sqlite3.connect(users_sensors_db) as conn:

            df_sensors = pd.read_sql('select sensor_display_name, sensor_name, sid from sensorstable', con=conn)  

            # センサー毎のdf生成
            def find_sensor_dataframe(sensor_name, sensor_display_name, columns, df):
                if sensor_name in selection_sensors:
                    sensors_sid = list(df_sensors["sid"].where(df_sensors["sensor_name"] == sensor_name))
                    sensor_sid = ''.join(str(s) for s in sensors_sid)
                    sensor_sid = sensor_sid.replace('a', '')
                    sensor_sid = sensor_sid.replace('n', '')
                    df = df[df["sid"] == sensor_sid]
                    df_power = df.filter(['power'], axis=1)
                    df_power = sensor_display_name + ' : 電池残量:  ' + str(df_power.iloc[1,-1])
                    
                    if "kasoku" in sensor_name:   
                        df = df.filter(['Date', 'x', 'y', 'z'], axis=1) 
                        # change timezone to Asia region 
                        df['Date'] = df['Date'] + pd.Timedelta(hours=9)
                        df = df.set_index('Date')
                        df = df.sort_values(['Date'])
                        df['x'] = df['x'].astype(float)
                        df['y'] = df['y'].astype(float)
                        df['z'] = df['z'].astype(float)

                        # グラフ表示範囲（取得）
                        df = df[-(graph_range * 12):]
                    
                        
                        # しきい値描画（仮運用）
                        if '+ Threshold' in columns_T:
                            columns.append('+ Threshold')
                            df['+ Threshold'] = 0.8
                        if '- Threshold' in columns_T:
                            columns.append('- Threshold')
                            df['- Threshold'] = -0.8

                        # 現在値・平均値
                        current_x_y_z = df.iloc[-1, :]
                        current_x_y_z = current_x_y_z[0], current_x_y_z[1], current_x_y_z[2]

                        avg_x_y_z = [round(df[s].mean(), 4) for s in list(df.columns.values)]
                        avg_x_y_z = avg_x_y_z[0], avg_x_y_z[1], avg_x_y_z[2]
                        
                        st.write("Cur(x,y,z):", current_x_y_z)
                        st.write("Avg(x,y,z):", avg_x_y_z)
                        
                    if "onsitu" in sensor_name:
                        df = df.filter(['Date', 'temp', 'humid'], axis=1)
                        # change timezone to Asia region 
                        df['Date'] = df['Date'] + pd.Timedelta(hours=9)
                        # set index in Date column
                        df = df.set_index('Date')
                    
                        # sort by datetime
                        df = df.sort_values(['Date'])
                        df['temp'] = df['temp'].astype(float)
                        df['humid'] = df['humid'].astype(float)
            
                        current_humid_temp = df.iloc[-1, :]
                        current_humid_temp = current_humid_temp[0], current_humid_temp[1]
                        
                        avg_humid_temp = [round(df[s].mean(), 4) for s in list(df.columns.values)]
                        avg_humid_temp = avg_humid_temp[0], avg_humid_temp[1]
                        
                        st.write("Cur(temp, humid):", current_humid_temp)
                        st.write("Avg(temp, humid):", avg_humid_temp)

                        df = df[-(graph_range * 12):]
                    
                    #　グラフ表示関数実行
                    show_graph(df, df_power, columns)


            def show_graph(df, df_power, columns):
                
                df = df.filter(columns, axis=1)
                df = df.reset_index()
                df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
                df['Date'] = df['Date'] + pd.Timedelta(hours=15)

                df = pd.melt(df, id_vars='Date')
        
                chart = (
                        alt.Chart(df) 
                        .mark_line(opacity=1, clip=True)
                        .encode(
                            x="Date:T",
                            y=alt.Y("value:Q", stack=None),
                            color='variable:N'
                            ).properties(
                                title=(df_power)
                            )
                        )
                

                st.altair_chart(chart, use_container_width=True)



        ######################## サイドバー ########################
        sensors = [i for i in df_sensors["sensor_name"]]
        sensors_display_name_list = [j for j in df_sensors["sensor_display_name"]]
        sensors_dic = {sensors[k]: sensors_display_name_list[k] for k in range(len(sensors))}

        # 加速度カラム
        selection_columns_kasoku = ["x", "y", "z"]

        # 温湿度カラム
        selection_columns_onsitudo = ["temp", "humid"]

        # しきい値カラム
        selection_columns_T = ['+ Threshold', '- Threshold']


        # ---- ログイン・ログアウト
        # authenticator.logout("Logout", "sidebar")
        # st.sidebar.title(f"ようこそ {name} さん")

        # ---- 表示センサー選択
        st.sidebar.success(f"ようこそ {st.session_state['name']} さん")
                
        st.sidebar.header("""表示センサー選択""")
            
        selection_sensors = st.sidebar.multiselect(
            'センサー名を選択してください。', 
            sensors_dic,
            sensors[0],
        )

        try:
            if not selection_sensors:
                st.error('少なくとも１つのセンサーを選んでください。')
            else:
                # グラフ表示範囲
                graph_range = st.sidebar.slider(label="グラフ表示範囲(分）", min_value=1, max_value=60, value=1, step=1)

                selected_kasoku_count = len([s for s in selection_sensors if 'kasoku' in s])
                selected_onsitudo_count = len([o for o in selection_sensors if 'onsitu' in o])

                # x, y, z軸表示・非表示
                if selected_kasoku_count > 0:
                    selection_columns_kasoku = st.sidebar.multiselect(
                        '表示軸 （加速度）を選択してください。', 
                        selection_columns_kasoku,
                        selection_columns_kasoku,
                        )
                
                    
                # temp, humid 表示・非表示
                # 温湿度センサ表示が1つ以上表示されている場合
                if selected_onsitudo_count > 0:
                    selection_columns_onsitudo = st.sidebar.multiselect(
                        '表示軸（温湿度）を選択してください。', 
                        selection_columns_onsitudo,
                        selection_columns_onsitudo,
                        )

                
                # +T, -T 表示・非表示
                # 加速度センサ表示が1つ以上表示されている場合
                if selected_kasoku_count > 0:
                    columns_T = st.sidebar.multiselect(
                        'しきい値(加速度）を選択してください。', 
                        selection_columns_T
                        )


                
                # センサーが2種類以上
                if selected_kasoku_count > 0 and selected_onsitudo_count > 0:
                    left_column, right_column = st.columns(2)
                    for sensor in selection_sensors:
                        if "kasoku" in sensor:
                            with left_column:
                                sensor_name = sensor
                                sensor_display_name = sensors_dic[sensor]
                                find_sensor_dataframe(sensor_name, sensor_display_name, selection_columns_kasoku, df)

                        if "onsitu" in sensor:
                            with right_column:
                                sensor_name = sensor
                                sensor_display_name = sensors_dic[sensor]
                                # st.write(selected_onsitudo_count)
                                find_sensor_dataframe(sensor_name, sensor_display_name, selection_columns_onsitudo, df)


                # センサーが1種類の場合
                else:
                    if not "onsitu" in selection_sensors:
                        for sensor in selection_sensors:
                            if "kasoku" in sensor:
                                sensor_name = sensor
                                sensor_display_name = sensors_dic[sensor]

                                find_sensor_dataframe(sensor_name, sensor_display_name, selection_columns_kasoku, df)

                    
                    if not "kasoku" in selection_sensors:
                        for sensor in selection_sensors:
                            if "onsitudo" in sensor:
                                sensor_name = sensor
                                sensor_display_name = sensors_dic[sensor]
                                find_sensor_dataframe(sensor_name, sensor_display_name, selection_columns_onsitudo, df)

        except:
            ValueError



        ######################## PDF / CSV作成 ########################
        st.sidebar.header("""PDF / CSV作成""")

        start_date = st.sidebar.date_input('開始日')
        end_date = st.sidebar.date_input('終了日')

        sensors_pdf = [i for i in df_sensors["sensor_name"]]
        selection_sensors_reports = st.sidebar.multiselect('対象センサーを選択してください。', sensors_pdf, sensors_pdf[0])


        # しきい値表示（PDF)作成ボタン
        # show_sikii_settings = st.sidebar.button("しきい値設定")
        show_sikii_settings = st.sidebar.radio('しきい値設定　※PDFにしきい値を描画する場合は必ずしきい値設定を【表示】にしてください。', ('非表示', '表示'), horizontal=True)
        if show_sikii_settings == "表示":
            # しきい値スライダー表示
            # しきい値設定スライダー（加速度）
            kasoku_slider_X = st.sidebar.slider(label="しきい値【X】", min_value=0.00, max_value=5.00, value= 2.5, step=.01, format="%f")
            kasoku_slider_Y = st.sidebar.slider(label="しきい値【Y】", min_value=0.00, max_value=5.00, value= 2.5, step=.01, format="%f")
            kasoku_slider_Z = st.sidebar.slider(label="しきい値【Z】", min_value=0.00, max_value=5.00, value= 2.5, step=.01, format="%f")
            
            # しきい値設定スライダー（温湿度）
            onsitu_slider_temp = st.sidebar.slider(label="しきい値【温度】", min_value=0.00, max_value=40.00, value=20.0, step=.1, format="%f")
            onsitu_slider_humid = st.sidebar.slider(label="しきい値【湿度】", min_value=0.00, max_value=100.00, value= 50.0, step=.1, format="%f")


        # PDFしきい値追加ボタン
        add_sikii_pdf_button = st.sidebar.checkbox("PDFしきい値追加")

        # PDF作成ボタン
        create_pdf_button = st.sidebar.button("PDF作成")
                
        # CSV作成ボタン
        create_csv_button = st.sidebar.button("CSV作成")

        # アラートメール On/Off
        alert_mail_button = st.sidebar.radio("メールアラート ※しきい値設定を【表示】後にOn選択してください。", ("Off", "On"), horizontal=True)


        # PDF作成ボタン押下処理
        if create_pdf_button:

            store_path = r'C:\Users\Administrator\Downloads\charts.pdf'
            # store_path = '/home/ec2-user/charts.pdf'
            # # store_path = r'%userprofile%\Downloads\charts.pdf'
 
            pdf = PdfPages(store_path)

            for sensor_name in selection_sensors_reports:
                # need to up
                sensors_sid = list(df_sensors["sid"].where(df_sensors["sensor_name"] == sensor_name))
                sensor_sid = ''.join(str(s) for s in sensors_sid)
                sensor_sid = sensor_sid.replace('a', '')
                sensor_sid = sensor_sid.replace('n', '')

                pdf_df = df.copy()
                pdf_df = pdf_df[(pdf_df['Date'] >= dt.datetime(start_date.year,start_date.month,start_date.day)) & (pdf_df['Date'] <= dt.datetime(end_date.year,end_date.month,end_date.day+1))]
                pdf_df = pdf_df.sort_values(['Date'])
                pdf_df = pdf_df[pdf_df['sid'] == sensor_sid]
                pdf_df = pdf_df.dropna(axis=1)
                pdf_df = pdf_df.drop(['power', 'sid'], axis=1)

                x = pdf_df.iloc[:, 0]
                fig, axes = plt.subplots()


                # CUE(加速度）が選択されている場合
                if "kasoku" in sensor_name:
                    la, = axes.plot(x, pdf_df["x"], label="x")
                    la, = axes.plot(x, pdf_df["y"], label="y")
                    la, = axes.plot(x, pdf_df["z"], label="z")
                    axes.legend()


                    # if show_sikii_settings:
                    if add_sikii_pdf_button:
                    # しきい値描画（全体を通じてのしきい値なので、x,y,z毎のしきい値を描画する？　20230130）
                        axes.axhline(y=kasoku_slider_X, color='r', linestyle='-')
                        axes.axhline(y=kasoku_slider_Y, color='r', linestyle='-')
                        axes.axhline(y=kasoku_slider_Z, color='r', linestyle='-')
                        

                #　ARE(温湿度場合)が選択されている場合
                if "onsitu" in sensor_name:
                    la, = axes.plot(x, pdf_df["temp"], label="Temp")
                    la, = axes.plot(x, pdf_df["humid"], label="Humid")
                    axes.legend()
                    
                    # 温湿度しきい値描画
                    if add_sikii_pdf_button:
                        # st.write(onsitu_slider_temp)
                        axes.axhline(y=onsitu_slider_temp, color='r', linestyle='-')
                        axes.axhline(y=onsitu_slider_humid, color='r', linestyle='-')

                axes.set_title('Sensor-{}'.format(sensor_name))
                axes.set_xlabel('Dateteime')
                axes.set_ylabel('Acceleration')
                fig.tight_layout()
                pdf.savefig(fig)

            pdf.close()


        # XLS作成ボタン押下処理
        if create_csv_button:
            # store_path = r'C:\Users\Administrator\Downloads\report.xlsx'
            # store_path = r'C:\report.xlsx'
            store_path = r'%userprofile%\Downloads\report.xlsx'
            writer = pd.ExcelWriter(store_path, engine= 'xlsxwriter')

            # 選択されたセンサー分エクセルシート作成用ループ処理
            with pd.ExcelWriter(store_path, engine= 'xlsxwriter') as writer:
                for sensor_name in selection_sensors_reports:
                    # need to up
                    sensors_sid = list(df_sensors["sid"].where(df_sensors["sensor_name"] == sensor_name))
                    sensor_sid = ''.join(str(s) for s in sensors_sid)
                    sensor_sid = sensor_sid.replace('a', '')
                    sensor_sid = sensor_sid.replace('n', '')

                    xls_df = df.copy()
                    xls_df = xls_df[(xls_df['Date'] >= dt.datetime(start_date.year,start_date.month,start_date.day)) & (xls_df['Date'] <= dt.datetime(end_date.year,end_date.month,end_date.day+1))]
                    xls_df = xls_df.sort_values(['Date'])
                    xls_df = xls_df[xls_df['sid'] == sensor_sid]
                    xls_df = xls_df.dropna(axis=1)
                    xls_df = xls_df.drop(['power', 'sid'], axis=1)

                    # 日時を年月日時分秒に分割
                    xls_df = xls_df.reset_index()
                    xls_df['year'] =xls_df['Date'].dt.year
                    xls_df['month'] = xls_df['Date'].dt.month
                    xls_df['day'] = xls_df['Date'].dt.day
                    xls_df['time'] = xls_df['Date'].dt.strftime("%H:%M:%S")
                    xls_df.pop('index')
                    xls_df.pop('Date')

                    if "kasoku" in sensor_name:
                        xls_df = xls_df[['year', 'month', 'day', 'time', 'x', 'y', 'z']]
                    if "onsitu" in sensor_name:
                        xls_df = xls_df[['year', 'month', 'day', 'time', 'temp', 'humid']]                            

                    xls_df.to_excel(writer, sheet_name = sensor_name)



        # x取得
        def create_alert_message(sensor_key, get_time_sensor_key, sensor_key_value, slider_value):
            if 'kasoku' in sensor_key:
                subject = "しきい値:{} {}を超える：{} が {} 検出されました".format(sensor_key, slider_value, sensor_key_value, get_time_sensor_key)    
                send_message(subject)
                    
            if 'humid' in sensor_key:
                subject = "しきい値（Humid）{} : {}度を超える:{} 度が {} 検出されました".format(sensor_key, slider_value, sensor_key_value, get_time_sensor_key)
                send_message(subject)
                    
            if 'temp' in sensor_key:    
                subject = "しきい値（Temp）{} : {}度を超える:{} 度が {} 検出されました".format(sensor_key, slider_value, sensor_key_value, get_time_sensor_key)
                send_message(subject)
                            
                        
        def send_message(subject):
            
            # ※Googleメール　1日の送信数に制限あり
            sender_email = "*******@gmail.com"
            rec_email = ["*******@gmail.com"] # 複数可能
            password = "*******" # アプリパスワード（2段階認証ONにし、パスワード生成）
            # アプリ パスワードでログインする
            # https://support.google.com/accounts/answer/185833?hl=ja
            message = "Values exceeding the standard values are detected."

            msg = MIMEText(message)
            msg['Subject'] = subject
            msg['From'] = sender_email
            msg['To'] = ', '.join(rec_email)

            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, rec_email, msg.as_string())



        # メール通知 on / off 追加予定
        if alert_mail_button == "On":
            
            # センサー名が選択されている場合(加速度)

            for sensor_name in df_sensors["sensor_name"]:
                sensors_sid = list(df_sensors["sid"].where(df_sensors["sensor_name"] == sensor_name))
                sensor_sid = ''.join(str(s) for s in sensors_sid)
                sensor_sid = sensor_sid.replace('a', '')
                sensor_sid = sensor_sid.replace('n', '')

                mail_df = df.copy()
                mail_df = mail_df[(mail_df['Date'] >= dt.datetime(start_date.year,start_date.month,start_date.day)) & (mail_df['Date'] <= dt.datetime(end_date.year,end_date.month,end_date.day+1))]
                mail_df = mail_df.sort_values(['Date'])
                mail_df = mail_df[mail_df['sid'] == sensor_sid]
                mail_df = mail_df.dropna(axis=1)
                mail_df = mail_df.drop(['power', 'sid'], axis=1)

                
                if "kasoku" in sensor_name:
                    if not mail_df.empty:
                        for s, n in zip(["_X", "_Y", "_Z"], range(1, len(mail_df.columns))):
                            sensor_key = sensor_name + s
                            get_time_sensor_key = mail_df.iloc[-1].values[0]
                            sensor_key_value = mail_df.iloc[-1].values[n]
                            
                            # しきい値を超えた場合、create_alert_messageを実行
                            if abs(sensor_key_value) > kasoku_slider_X:
                                create_alert_message(sensor_key, get_time_sensor_key, sensor_key_value, kasoku_slider_Z)

                            if abs(sensor_key_value) > kasoku_slider_Y:
                                create_alert_message(sensor_key, get_time_sensor_key, sensor_key_value, kasoku_slider_Z)
                            
                            if abs(sensor_key_value) > kasoku_slider_Z:
                                create_alert_message(sensor_key, get_time_sensor_key, sensor_key_value, kasoku_slider_Z)
                        

                if "onsitu" in sensor_name:
                    if not mail_df.empty:
                        for s, n in zip(["_temp", "_humid"], range(1, len(mail_df.columns))):
                            sensor_key = sensor_name + s
                            get_time_sensor_key = mail_df.iloc[-1].values[0]
                            sensor_key_value_temp = mail_df.iloc[-1].values[1]
                            sensor_key_value_humid = mail_df.iloc[-1].values[2]
                                
                            # しきい値を超えた場合、send_messageを実行
                            if sensor_key_value_temp >= onsitu_slider_temp:
                                create_alert_message(sensor_key, get_time_sensor_key, sensor_key_value_temp, onsitu_slider_temp)

                            if sensor_key_value_humid >= onsitu_slider_humid:
                                create_alert_message(sensor_key, get_time_sensor_key, sensor_key_value_humid, onsitu_slider_humid)

    else:
        users_sensors_db = 'C:\\meiji_aws_product\db\\users_sensors.db'
        # users_sensors_db = '/home/ec2-user/meiji_aws_product/db/users_sensors.db'

        login.Login(users_sensors_db)          

