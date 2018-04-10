from operators.hive_email_operators import HiveEmailOperator
from operators.hive_operators import HiveDuplicateCheckOperator
from operators.java_operators import JavaOperator
from operators.presto_operators import PrestoOperationCheckOperator, PrestoDuplicateCheckOperator, PrestoCountCheckOperator
from operators.sudo_bash_operators import SudoBashOperator

PANDORA_OPERATORS = [
    HiveDuplicateCheckOperator,
    HiveEmailOperator,
    JavaOperator,
    PrestoCountCheckOperator,
    PrestoDuplicateCheckOperator,
    PrestoOperationCheckOperator,
    SudoBashOperator,
]
