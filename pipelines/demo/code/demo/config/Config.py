from demo.graph.risk_scoring.config.Config import SubgraphConfig as risk_scoring_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, risk_scoring: dict=None, **kwargs):
        self.spark = None
        self.update(risk_scoring)

    def update(self, risk_scoring: dict={}, **kwargs):
        prophecy_spark = self.spark
        self.risk_scoring = self.get_config_object(
            prophecy_spark, 
            risk_scoring_Config(prophecy_spark = prophecy_spark), 
            risk_scoring, 
            risk_scoring_Config
        )
        pass
