from enum import Enum
from providers.bird import Bird
from providers.circ import Circ
from providers.frontend import Frontend
from providers.lime import Lime
from providers.tier import Tier
from providers.voi import Voi

class Providers(Enum):
    BIRD = Bird()
    CIRC = Circ()
    LIME = Lime()
    TIER = Tier()
    VOI = Voi()
    HIVE = Frontend("hive")
    SCOOTA = Frontend("scoota")
    ZERO = Frontend("zero")

    @property
    def name(self):
        return self.value.name

