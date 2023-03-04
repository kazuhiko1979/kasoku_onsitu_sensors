import time 
import sqlite3 

import bcrypt
from PIL import Image
import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth


##### Models #####
class ConnectDataBase:
    def __init__(self, db_path):
        self._db_path = db_path
        self.conn = sqlite3.connect(self._db_path)
        self.cursor = self.conn.cursor()
        self.df = None
        # self.df_sensor = None
        

    def get_table(self, table="userstable", key="*"):
        self.df = pd.read_sql(f'SELECT {key} FROM {table}', self.conn)
        return self.df

    def get_sensors_table(self, table="sensorstable", key="*"):
        self.df_sensor = pd.read_sql(f'SELECT {key} FROM {table}', self.conn)
        # self.df = pd.read_sql(f'SELECT {key} FROM {table}', self.conn)
        return self.df_sensor

    def close(self):
        self.cursor.close()
        self.conn.close()

    def __del__(self):
        self.close()

class UserDataBase(ConnectDataBase):
    def __init__(self, db_path):
        super().__init__(db_path)
        # users_sensors_db:カラム名
        self.__name = "name"
        self.__username = "username"
        self.__password =  "password"
        self.__admin = "admin"

        self.__create_user_table()
        self.get_table()
        
        self.__sensor_display_name = "sensor_display_name"
        self.__sensor_name = "sensor_name"
        self.__sid =  "sid"
        
        self.__create_sensor_table()
        self.get_sensors_table()
        self.get_table()

    @property
    def name(self):
        return self.__name  
    @property
    def username(self):
        return self.__username  
    @property
    def password(self):
        return self.__password  
    @property
    def admin(self):
        return self.__admin

    @property
    def sensor_display_name(self):
        return self.__sensor_display_name  
    @property
    def sensor_name(self):
        return self.__sensor_name  
    @property
    def sid(self):
        return self.__sid 

    def __create_user_table(self):
        """
        該当テーブルが無ければ作る
        """
        self.cursor.execute('CREATE TABLE IF NOT EXISTS userstable({} TEXT, {} TEXT unique, {} TEXT, {} INT)'.format(self.name, self.username, self.password, self.admin))
        

    def __create_sensor_table(self):
        """
        該当テーブルが無ければ作る
        """
        self.cursor.execute('CREATE TABLE IF NOT EXISTS sensorstable({} TEXT, {} TEXT unique, {} INT)'.format(self.sensor_display_name, self.sensor_name, self.sid))


    def _hashing_password(self, plain_password):
        return bcrypt.hashpw(plain_password.encode(), bcrypt.gensalt()).decode()


    def __chk_username_existence(self, username):
        """
        ユニークユーザの確認
        """
        self.cursor.execute('select {} from userstable'.format(self.username))
        exists_users = [_[0] for _ in self.cursor]
        if username in exists_users:
            return True

    def __chk_delete_username_existence(self, delete_username):
        """
        ユニークユーザの確認
        """
        self.cursor.execute('select {} from userstable'.format(self.username))
        exists_users = [_[0] for _ in self.cursor]
        if delete_username in exists_users:
            return True

    def __chk_sensorname_existence(self, sensor_name):
        """
        ユニークセンサーの確認
        """
        self.cursor.execute('select {} from sensorstable'.format(self.sensor_name))
        exists_users = [_[0] for _ in self.cursor]
        if sensor_name in exists_users:
            return True

    def __chk_delete_sensorname_existence(self, delete_sensorname):
        """
        ユニークセンサーの確認
        """
        self.cursor.execute('select {} from sensorstable'.format(self.sensor_name))
        exists_sensors = [_[0] for _ in self.cursor]
        if delete_sensorname in exists_sensors:
            return True

    
    # ユーザー登録
    def add_user(self, name, username, password, admin):
        """
        新しくユーザを追加します
            [args]
                [0] name: str
                [1] username : str (unique)
                [2] password : str
                [3] admin : bool
            [return]
                res: str or None
        """

        if name=="" or username=="" or password=="":
            return
        if self.__chk_username_existence(username):
            return 
        
        # 登録
        hashed_password = self._hashing_password(password)
        self.cursor.execute('INSERT INTO userstable({}, {}, {}, {}) VALUES (?, ?, ?, ?)'.format(self.name, self.username, self.password, self.admin),
                                (name, username, hashed_password, int(admin)))
        self.conn.commit()

        return f"{name}さんのアカウントを作成しました"

    # ユーザー削除
    def delete_user(self, delete_username):
        if not delete_username == "":
            # return
            if self.__chk_delete_username_existence(delete_username):
                sql = "DELETE FROM userstable WHERE username = '{}'".format(delete_username)
                self.cursor.execute(sql)
                self.conn.commit()

                # self.get_table()
                # st.table(self.df.drop(self.password, axis=1))
                # self.get_table()
                # self.username.empty()

                return "アカウントを削除しました"


    # センサー登録
    def add_sensor(self, sensor_display_name, sensor_name, sid):
        """
        新しくセンサーを追加します
            [args]
                [0] sensor_display_name: str
                [1] sensor_name : str (unique)
                [2] sid : int
            [return]
                res: str or None
        """

        if sensor_display_name=="" or sensor_name=="" or sid=="":
            return
        if self.__chk_sensorname_existence(sensor_name):
            return

        # 登録
        self.cursor.execute('INSERT INTO sensorstable({}, {}, {}) VALUES (?, ?, ?)'.format(self.sensor_display_name, self.sensor_name, self.sid),
                                # (sensor_display_name, sensor_name, int(sid)))
                                (sensor_display_name, sensor_name, sid))
        self.conn.commit()
        
        return f"{sensor_name}のセンサーを作成しました"

    # センサー削除
    def delete_sensor(self, delete_sensorname):
        if not delete_sensorname == "":
            # return
            if self.__chk_delete_sensorname_existence(delete_sensorname):
                sql = "DELETE FROM sensorstable WHERE sensor_name = '{}'".format(delete_sensorname)
                self.cursor.execute(sql)
                self.conn.commit()

                return "センサーを削除しました"

##### Views #####
class AlwaysView:
    def __init__(self):
        self.main_menu = ["Login", "Admin", "Contact"]
        self.choice_menu = st.sidebar.selectbox("メニュー", self.main_menu)


class GeneralUserView:
    def main_form(self):
        st.header("ようこそ！")
        # logo = Image.open('/home/ec2-user/meiji_aws_product/img/login/title_logo2.png')
        logo = Image.open('C:\\meiji_aws_product\\img\\login\\title_logo2.png')

        st.image(logo, use_column_width=False)

    def side_form(self, model):
        """
        認証フォームの表示
        """
        self.authenticator = stauth.Authenticate(
            model.df[model.name],
            model.df[model.username],
            model.df[model.password],
            'some_cookie_name', 
            'some_signature_key', 
            cookie_expiry_days=0)
        self.authenticator.login("ログイン", "sidebar")


class AdminUserView:
    def main_form(self, model):
        # user登録・削除・一覧
        with st.form(key="create_acount", clear_on_submit=True):
            st.subheader("新規ユーザの作成")
            self.name = st.text_input("ニックネームを入力してください", key="create_user")
            self.create_username = st.text_input("ユーザー名(ID)を入力してください", key="create_id")
            self.password = st.text_input("パスワードを入力してください",type='password', key="create_pass")
            self.adminauth = st.checkbox("管理者権限の付与")
            self.submit = st.form_submit_button(label='アカウントの作成')
        
        with st.form(key="delete_acount", clear_on_submit=True):
            st.subheader("ユーザの削除")
            self.delete_username = st.text_input("削除するユーザー名(ID)を入力してください", key="delete_id")
            self.delete = st.form_submit_button(label='アカウントの削除')
            
        with st.form(key="show_table"):
            self.show_table = st.form_submit_button(label='ユーザー覧')
            if self.show_table:
                st.table(model.df.drop(model.password, axis=1))


        # sensor登録・削除・一覧
        with st.form(key="create_sensor", clear_on_submit=True):
            st.subheader("新規センサーの作成")
            self.sensor_display_name = st.text_input("グラフタイトル表示用センサー名を入力してください", key="create_sensor_display_name")
            self.sensor_name = st.text_input("センサー名を入力してください", key="create_sensor_name")
            self.sid = st.text_input("センサー(ID)を入力してください", key="create_sid")
            self.sensor_submit = st.form_submit_button(label='センサーの作成')

        with st.form(key="delete_sensor", clear_on_submit=True):
            st.subheader("センサーの削除")
            self.delete_sensorname = st.text_input("削除するセンサー名を入力してください", key="delete_sensorname")
            self.sensor_delete = st.form_submit_button(label='センサーの削除')
    
        with st.form(key="show_sensor_table"):
            self.show_sensor_table = st.form_submit_button(label='センサ一覧')
            if self.show_sensor_table:
                st.table(model.df_sensor)

        self.emp = st.empty()

    def side_form(self):
        st.sidebar.write("---")
        st.sidebar.info("adminがキーです")
        return  st.sidebar.text_input("管理者アクセスキー" ,type='password')


class ContactView:
    def _main_form(self):
        st.subheader("お問い合わせ先")
        st.write("""
                |連絡先名 | お客様サポート |  
                |:--:|:--:|
                |電話番号| 0000-0000-0000 |   
                |メール| meiji_kikai@example.com |  
        """)


##### Controller #####
class LoginController:
    def __init__(self, db_path):
        self.model = UserDataBase(db_path)  
        self.av = AlwaysView()
        self.gu = GeneralUserView()
        self.au = AdminUserView()
        # self.su = SensorView()
        self.cv = ContactView()

    # 各ページのコントロール
    def _general(self):
        """
        アカウント認証が成功している場合st_sessionが更新される
        """
        self.gu.main_form()
        self.gu.side_form(self.model)
        auth = 'authentication_status'

        # アカウント認証に成功
        if st.session_state[auth]:
            st.success(f"ようこそ {st.session_state['name']} さん")
            with st.spinner('アカウント情報を検証中...'):
                time.sleep(0.2)

        # アカウント認証の情報が間違っているとき
        elif st.session_state[auth] == False:
            st.error("ログイン情報に誤りがあります。再度入力確認してください。")
            st.warning("アカウントをお持ちでない方は管理者に連絡しアカウントを作成してください")

        # アカウント認証の情報が何も入力されていないとき
        elif st.session_state[auth] is None:
            st.warning("アカウント情報を入力してログインしてください。")

    def _admin(self):
        admin_chk = self.au.side_form()
        # パスべた書き
        if admin_chk == "admin":
            self.au.main_form(self.model)
            # self.au.main_form(self.model_sensor)
            # ユーザ登録・削除
            if self.au.submit:
                res = self.model.add_user(self.au.name, self.au.create_username, self.au.password, self.au.adminauth)
                if res:
                    self.au.emp.success(res)
                else:
                    self.au.emp.warning("入力値に問題があるため、登録出来ませんでした")

            if self.au.delete:
                res = self.model.delete_user(self.au.delete_username)
                if res:
                    self.au.emp.success(res)
                else:
                    self.au.emp.warning("入力値に問題があるため、削除出来ませんでした")
            
            # センサー登録・削除
            if self.au.sensor_submit:
                res = self.model.add_sensor(self.au.sensor_display_name, self.au.sensor_name, self.au.sid)
                if res:
                    self.au.emp.success(res)
                else:
                    self.au.emp.warning("入力値に問題があるため、登録出来ませんでした")

            if self.au.sensor_delete:
                # res = self.model_sensor.delete_sensor(self.au.delete_sensorname)
                res = self.model.delete_sensor(self.au.delete_sensorname)
                if res:
                    self.au.emp.success(res)
                else:
                    self.au.emp.warning("入力値に問題があるため、削除出来ませんでした")

            
        elif admin_chk == "":
            st.subheader("アクセスキーを入力してください")
        else:
            st.error("管理者キーが違います")

    # ページを切り替えた際に実行する関数を変える
    def page_choice(self):
        """
        ページの遷移
        """
        if self.av.choice_menu == self.av.main_menu[0]:
            self._general()
        if self.av.choice_menu == self.av.main_menu[1]:
            self._admin()
        if self.av.choice_menu == self.av.main_menu[2]:
            self.cv._main_form()
        

##### Main #####
class Login:
    def __init__(self, db_path):
        self.controller = LoginController(db_path)
        self.controller.page_choice()