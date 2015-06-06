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
    
    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['candidacies'] = [c.as_dict() for c in self.candidacies]
        return d

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
    
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

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

    def as_dict(self):
        d = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        d['officers'] = [o.as_dict() for o in self.officers]

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
    
    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class FiledDoc(Base):
    __tablename__ = 'filed_docs'
    id = sa.Column(sa.Integer, primary_key=True)

    committee_id = sa.Column(sa.Integer, sa.ForeignKey('committees.id'))
    committee = sa.orm.relationship('Committee', backref='filed_docs')
    
    doc_type = sa.Column(sa.String(10))
    doc_name = sa.Column(sa.String(30))
    amended = sa.Column(sa.Boolean)
    comment = sa.Column(sa.String(100))
    page_count = sa.Column(sa.Integer)
    election_type = sa.Column(sa.String)
    election_year = sa.Column(sa.Integer)
    reporting_period_begin = sa.Column(sa.DateTime, index=True)
    reporting_period_end = sa.Column(sa.DateTime, index=True)
    received_at = sa.Column(sa.String)
    received_datetime = sa.Column(sa.DateTime, index=True)
    source = sa.Column(sa.String)
    provider = sa.Column(sa.String)
    signer_last_name = sa.Column(sa.String)
    signer_first_name = sa.Column(sa.String)
    submitter_last_name = sa.Column(sa.String)
    submitter_first_name = sa.Column(sa.String)
    submitter_address1 = sa.Column(sa.String)
    submitter_address2 = sa.Column(sa.String)
    submitter_city = sa.Column(sa.String)
    submitter_state = sa.Column(sa.String)
    submitter_zip = sa.Column(sa.String)
    b9_signer_last_name = sa.Column(sa.String)
    b9_signer_first_name = sa.Column(sa.String)
    archived = sa.Column(sa.Boolean)
    clarification = sa.Column(sa.Text)
    redaction_requested = sa.Column(sa.Boolean)

    def __repr__(self):
        return '<FiledDoc %r>' % (self.id)

class D2Report(Base):
    __tablename__ = 'd2_reports'
    id = sa.Column(sa.Integer, primary_key=True)
    
    # Not making an explicit relations here because there are reports
    # that have related filed_docs and committees that don't exist, apparently
    committee_id = sa.Column(sa.Integer)
    filed_doc_id = sa.Column(sa.Integer)
    
    beginning_funds_avail = sa.Column(DOUBLE_PRECISION)
    individual_itemized_contrib = sa.Column(DOUBLE_PRECISION)
    individual_non_itemized_contrib = sa.Column(DOUBLE_PRECISION)
    transfer_in_itemized = sa.Column(DOUBLE_PRECISION)
    transfer_in_non_itemized = sa.Column(DOUBLE_PRECISION)
    loan_received_itemized = sa.Column(DOUBLE_PRECISION)
    loan_received_non_itemized = sa.Column(DOUBLE_PRECISION)
    other_receipts_itemized = sa.Column(DOUBLE_PRECISION)
    other_receipts_non_itemized = sa.Column(DOUBLE_PRECISION)
    total_receipts = sa.Column(DOUBLE_PRECISION)
    inkind_itemized = sa.Column(DOUBLE_PRECISION)
    inkind_non_itemized = sa.Column(DOUBLE_PRECISION)
    total_inkind = sa.Column(DOUBLE_PRECISION)
    transfer_out_itemized = sa.Column(DOUBLE_PRECISION)
    transfer_out_non_itemized = sa.Column(DOUBLE_PRECISION)
    loan_made_itemized = sa.Column(DOUBLE_PRECISION)
    loan_made_non_itemized = sa.Column(DOUBLE_PRECISION)
    expenditures_itemized = sa.Column(DOUBLE_PRECISION)
    expenditures_non_itemized = sa.Column(DOUBLE_PRECISION)
    independent_expenditures_itemized = sa.Column(DOUBLE_PRECISION)
    independent_expenditures_non_itemized = sa.Column(DOUBLE_PRECISION)
    total_expenditures = sa.Column(DOUBLE_PRECISION)
    debts_itemized = sa.Column(DOUBLE_PRECISION)
    debts_non_itemized = sa.Column(DOUBLE_PRECISION)
    total_debts = sa.Column(DOUBLE_PRECISION)
    total_investments = sa.Column(DOUBLE_PRECISION)
    end_funds_available = sa.Column(DOUBLE_PRECISION)
    archived = sa.Column(sa.Boolean)

    def __repr__(self):
        return '<D2Report %r>' % (self.id)


class Receipt(Base):
    __tablename__ = 'receipts'
    id = sa.Column(sa.Integer, primary_key=True)
    
    committee_id = sa.Column(sa.Integer, sa.ForeignKey('committees.id'))
    committee = sa.orm.relationship('Committee', backref='receipts')
    
    filed_doc_id = sa.Column(sa.Integer, sa.ForeignKey('filed_docs.id'))
    filed_doc = sa.orm.relationship('FiledDoc', backref='receipts')
    
    etrans_id = sa.Column(sa.String)
    last_name = sa.Column(sa.String)
    first_name = sa.Column(sa.String)
    received_date = sa.Column(sa.DateTime)
    amount = sa.Column(DOUBLE_PRECISION)
    aggregate_amount = sa.Column(DOUBLE_PRECISION)
    loan_amount = sa.Column(DOUBLE_PRECISION)
    occupation = sa.Column(sa.String)
    employer = sa.Column(sa.String)
    address1 = sa.Column(sa.String)
    address2 = sa.Column(sa.String)
    city = sa.Column(sa.String)
    state = sa.Column(sa.String)
    zipcode = sa.Column(sa.String)
    d2_part = sa.Column(sa.String)
    description = sa.Column(sa.Text)
    vendor_last_name = sa.Column(sa.String)
    vendor_first_name = sa.Column(sa.String)
    vendor_address1 = sa.Column(sa.String)
    vendor_address2 = sa.Column(sa.String)
    vendor_city = sa.Column(sa.String)
    vendor_state = sa.Column(sa.String)
    vendor_zipcode = sa.Column(sa.String)
    archived = sa.Column(sa.Boolean)
    country = sa.Column(sa.String)
    redaction_requested = sa.Column(sa.Boolean)

    def __repr__(self):
        return '<Receipt %r>' % self.id

class Expenditure(Base):
    __tablename__ = 'expenditures'
    id = sa.Column(sa.Integer, primary_key=True)
    
    committee_id = sa.Column(sa.Integer, sa.ForeignKey('committees.id'))
    committee = sa.orm.relationship('Committee', backref='expenditures')
    
    filed_doc_id = sa.Column(sa.Integer, sa.ForeignKey('filed_docs.id'))
    filed_doc = sa.orm.relationship('FiledDoc', backref='expenditures')

    etrans_id = sa.Column(sa.String)
    last_name = sa.Column(sa.String)
    first_name = sa.Column(sa.String)
    expended_date = sa.Column(sa.DateTime)
    amount = sa.Column(DOUBLE_PRECISION)
    aggregate_amount = sa.Column(DOUBLE_PRECISION)
    address1 = sa.Column(sa.String)
    address2 = sa.Column(sa.String)
    city = sa.Column(sa.String)
    state = sa.Column(sa.String)
    zipcode = sa.Column(sa.String)
    d2_part = sa.Column(sa.String)
    purpose = sa.Column(sa.String)
    candidate_name = sa.Column(sa.String)
    office = sa.Column(sa.String)
    supporting = sa.Column(sa.Boolean)
    opposing = sa.Column(sa.Boolean)
    archived = sa.Column(sa.Boolean)
    country = sa.Column(sa.String)
    redaction_requested = sa.Column(sa.Boolean)

    def __repr__(self):
        return '<Expenditure %r>' % self.id

class Investment(Base):
    __tablename__ = 'investments'
    id = sa.Column(sa.Integer, primary_key=True)
    
    committee_id = sa.Column(sa.Integer, sa.ForeignKey('committees.id'))
    committee = sa.orm.relationship('Committee', backref='investments')
    
    filed_doc_id = sa.Column(sa.Integer, sa.ForeignKey('filed_docs.id'))
    filed_doc = sa.orm.relationship('FiledDoc', backref='investments')

    description = sa.Column(sa.String)
    purchase_date = sa.Column(sa.Date)
    purchase_shares = sa.Column(sa.Integer)
    purchase_price = sa.Column(DOUBLE_PRECISION)
    current_value = sa.Column(DOUBLE_PRECISION)
    liquid_value = sa.Column(DOUBLE_PRECISION)
    liquid_date = sa.Column(sa.Date)
    last_name = sa.Column(sa.String)
    first_name = sa.Column(sa.String)
    address1 = sa.Column(sa.String)
    address2 = sa.Column(sa.String)
    city = sa.Column(sa.String)
    state = sa.Column(sa.String)
    zipcode = sa.Column(sa.String)
    archived = sa.Column(sa.Boolean)
    country = sa.Column(sa.String)

    def __repr__(self):
        return '<Investment %r>' % self.id


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

