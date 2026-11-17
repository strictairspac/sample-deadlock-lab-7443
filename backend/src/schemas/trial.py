from pydantic import BaseModel, Field
from src.constants.enums import TRIAL_KINDS
class TrialInput(BaseModel):
    name: str=Field(min_length=2,max_length=120)
    transfer_id: int=Field(gt=0)
    # 交叉演练第二笔事务的流转申请；为空即原单样本演练
    second_transfer_id: int|None=Field(default=None,gt=0)
    kind: str=Field(default='single')
    timeout_ms: int=Field(default=250,ge=50,le=2000)
    def model_post_init(self,ctx):
        if self.kind not in TRIAL_KINDS:
            raise ValueError('演练类型仅支持 single / cross')
