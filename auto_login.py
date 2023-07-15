import os
import time
import json
import win32com.client

from pywinauto import application


class AutoLogin:
    def __init__(self):
        self.obj_CpUtil_CpCybos = win32com.client.Dispatch('CpUtil.CpCybos')

    @property
    def config_connect(self):
        """
        Configure connection to creon.
        """
        connect = self.obj_CpUtil_CpCybos.Isconnect

        if connect == 0:
            print("CONNECTING TO CREON PLUS")
            return False
        return True

    @property
    def process_disconnect(self):
        """
        Disconnect creon.
        """
        if self.config_connect:
            self.obj_CpUtil_CpCybos.PlusDisconnect()

    @property
    def process_kill(self):
        """
        Kill creon.

        <description>
        taskkill: Kill process for Windows.
        wmic: Kill process for Windows + different OS.
        """
        os.system('taskkill /IM coStarter* /F /T')
        os.system('taskkill /IM CpStart* /F /T')
        os.system('taskkill /IM DibServer* /F /T')

        os.system('wmic process where "name like \'%coStarter%\'" call terminate')
        os.system('wmic process where "name like \'%CpStart%\'" call terminate')
        os.system('wmic process where "name like \'%DibServer%\'" call terminate')

    def process_connect(self, login_info: str):
        """
        Connect creon.

        <description>
        login_info: json file containing id, pwd, pwdcert.
        """
        with open(login_info, 'r') as info:
            login = json.load(info)

        id_, pwd, pwdcert = login["id"], login["pwd"], login["pwdcert"]

        def process_start():
            app = application.Application()
            app.start(
                "C:\CREON\STARTER\coStarter.exe /prj:cp /id:{id} /pwd:{pwd} /pwdcert:{pwdcert} /autostart".format(
                    id=id_, pwd=pwd, pwdcert=pwdcert
                )
            )

        if self.config_connect:
            self.process_disconnect
            self.process_kill
        process_start()

        while not self.config_connect:
            time.sleep(1)


if __name__ == "__main__":
    auto_login = AutoLogin()
    auto_login.process_connect('login.json')
