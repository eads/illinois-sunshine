from .database import Base, db_session as session
from flask_bcrypt import Bcrypt
import sqlalchemy as sa
from sqlalchemy.orm import synonym
from sqlalchemy.dialects.postgresql import ENUM, DOUBLE_PRECISION

bcrypt = Bcrypt()

class Candidate(Base):
    __tablename__ = 'candidates'
    id = sa.Column(sa.Integer, primary_key=True)
    ocd_id = sa.Column(sa.String)
    last_name = sa.Column(sa.String)
    first_name = sa.Column(sa.String)
    address_1 = sa.Column(sa.String)
    address_2 = sa.Column(sa.String)
    city = sa.Column(sa.String)
    state = sa.Column(sa.String)
    zipcode = sa.Column(sa.String)
    office = sa.Column(sa.String)
    district_type = sa.Column(sa.String)
    district = sa.Column(sa.String)
    residence_county = sa.Column(sa.String)
    party = sa.Column(sa.String)
    redaction_requested = sa.Column(sa.Boolean)
    
    date_added = sa.Column(sa.DateTime, server_default=sa.text('NOW()'))
    last_update = sa.Column(sa.DateTime, onupdate=sa.func.now())

    def __repr__(self):
        return '<Candidate %r %r>' % (self.first_name, self.last_name)

class Candidacy(Base):
    __tablename__ = 'candidacies'
    id = sa.Column(sa.Integer, primary_key=True)
    
    candidate_id = sa.Column(sa.Integer, sa.ForeignKey('candidates.id'))
    candidate = sa.orm.relationship('Candidate', backref='candidacies')
    
    election_type = sa.Column(sa.String)
    election_year = sa.Column(sa.Integer)
    # Incumbent, challenger, open seat
    race_type = sa.Column(ENUM('incumbent', 'challenger', 'open seat', 'retired',
                            name='candidacy_race_type'))
    outcome = sa.Column(ENUM('won', 'lost', name='candidacy_outcome'))
    fair_campaign = sa.Column(sa.Boolean)
    limits_off = sa.Column(sa.Boolean)
    limits_off_reason = sa.Column(sa.String)

    def __repr__(self):
        return '<Candidacy %r %r, (%r %r)>' % (self.candidate.first_name, 
                                               self.candidate.last_name, 
                                               self.election_year,
                                               self.election_type)

candidate_committees = sa.Table('candidate_committees', Base.metadata,
                       sa.Column('candidate_id', sa.Integer, sa.ForeignKey('candidates.id')),
                       sa.Column('committee_id', sa.Integer, sa.ForeignKey('committees.id'))
)

class Committee(Base):
    __tablename__ = 'committees'
    id = sa.Column(sa.Integer, primary_key=True)
    type = sa.Column(sa.String)
    state_committee = sa.Column(sa.Boolean)
    state_id = sa.Column(sa.Integer)
    local_committee = sa.Column(sa.Boolean)
    local_id = sa.Column(sa.Integer)
    refer_name = sa.Column(sa.String)
    name = sa.Column(sa.String)
    address1 = sa.Column(sa.String)
    address2 = sa.Column(sa.String)
    address3 = sa.Column(sa.String)
    city = sa.Column(sa.String)
    state = sa.Column(sa.String)
    zipcode = sa.Column(sa.String)
    active = sa.Column(sa.Boolean)
    status_date = sa.Column(sa.DateTime)
    creation_date = sa.Column(sa.DateTime)
    creation_amount = sa.Column(DOUBLE_PRECISION)
    disp_funds_return = sa.Column(sa.Boolean)
    disp_funds_political_committee = sa.Column(sa.Boolean)
    disp_funds_charity = sa.Column(sa.Boolean)
    disp_funds_95 = sa.Column(sa.Boolean)
    disp_funds_description = sa.Column(sa.Text)
    # These use the same ENUM. Need to create is separately
    candidate_position = sa.Column(ENUM('support', 'oppose', 
                                        name='committee_position',
                                        create_type=False))
    policy_position = sa.Column(ENUM('support', 'oppose', 
                                     name='committee_position',
                                     create_type=False))
    party = sa.Column(sa.String)
    purpose = sa.Column(sa.Text)

    candidates = sa.orm.relationship('Candidate', 
                                     secondary=candidate_committees, 
                                     backref='committees')

    def __repr__(self):
        return '<Committee %r>' % self.name

class Officer(Base):
    __tablename__ = 'officers'
    id = sa.Column(sa.Integer, primary_key=True)
    
    committee_id = sa.Column(sa.Integer, sa.ForeignKey('committees.id'))
    committee = sa.orm.relationship('Committee', backref='officers')
    
    last_name = sa.Column(sa.String)
    first_name = sa.Column(sa.String)
    address1 = sa.Column(sa.String)
    address2 = sa.Column(sa.String)
    city = sa.Column(sa.String)
    state = sa.Column(sa.String)
    zipcode = sa.Column(sa.String)
    title = sa.Column(sa.String)
    phone = sa.Column(sa.String)
    resign_date = sa.Column(sa.DateTime)
    redaction_requested = sa.Column(sa.Boolean)

    current = sa.Column(sa.Boolean)

    def __repr__(self):
        return '<Officer %r %r>' % (self.first_name, self.last_name)

class User(Base):
    __tablename__ = 'app_user'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String, nullable=False, unique=True)
    email = sa.Column(sa.String, nullable=False, unique=True)
    _password = sa.Column('password', sa.String, nullable=False)
    
    def __repr__(self): # pragma: no cover
        return '<User %r>' % self.name

    def _get_password(self):
        return self._password
    
    def _set_password(self, value):
        self._password = bcrypt.generate_password_hash(value)

    password = property(_get_password, _set_password)
    password = sa.orm.synonym('_password', descriptor=password)

    def __init__(self, name, email, password):
        self.name = name
        self.password = password
        self.email = email

    @classmethod
    def get_by_username(cls, name):
        return session.query(cls).filter(cls.name == name).first()

    @classmethod
    def check_password(cls, name, value):
        user = cls.get_by_username(name)
        if not user: # pragma: no cover
            return False
        return bcrypt.check_password_hash(user.password, value)

    def is_authenticated(self):
        return True

    def is_active(self):
        return True

    def is_anonymous(self):
        return False

    def get_id(self):
        return self.id

