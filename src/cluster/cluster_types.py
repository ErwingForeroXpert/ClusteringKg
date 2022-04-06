from enum import Enum, unique

@unique
class TYPE_CLUSTERS(Enum):
    DIRECTA = "directa"
    INDIRECTA = "indirecta"
    
    @classmethod
    def exist(cls, key):
        return key in cls.__members__ 
    