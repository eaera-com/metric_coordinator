class MT5ManagerClient():
    def __init__(self,server,login:str, password:str) -> None:
        import MT5Manager

        self.manager = MT5Manager.ManagerAPI()

        result = self.manager.Connect(
            server,
            login,
            password,
            MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_ACTIVITY
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_CLIENTS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_GATEWAYS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_GROUPS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_HOLIDAYS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_MAIL
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_NEWS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_ORDERS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_PLUGINS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_POSITIONS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_REQUESTS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_SUBSCRIPTIONS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_SYMBOLS
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_TIME
            | MT5Manager.ManagerAPI.EnPumpModes.PUMP_MODE_USERS,
            120000,
        )

        print("Connected to MT5:", result)

        def GetDealsByGroup(self, group, from_time, to_time):
            deals = self.manager.DealsGet(group, from_time, to_time)
            return deals
