class Provider():
    name = None
    required_settings = []
    frontend = False

    @classmethod
    def check_settings(self, settings):
        for setting in self.required_settings:
            if setting not in settings["PROVIDERS"]:
                raise Exception(f'{setting} has to be configured')

    @classmethod
    def get_scooters(self, settings, city):
        pass

