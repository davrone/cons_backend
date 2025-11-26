# simple operator selector stub
from sqlalchemy.orm import Session
from .. import models

def select_operator_for_consultation(db: Session, consultation: models.Consultation):
    # placeholder algorithm: pick first active user
    user = db.query(models.User).filter(models.User.deletion_mark == False).first()
    if user:
        consultation.manager = user.account_id
        db.add(consultation)
        db.commit()
        db.refresh(consultation)
    return consultation.manager
