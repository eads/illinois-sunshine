def format_money(s):
    import locale
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    return locale.currency(s, grouping=True)

def format_money_short(n):
    import math
    millnames=['','K','M','B']
    n = float(n)
    millidx=max(0,min(len(millnames)-1,
                      int(math.floor(math.log10(abs(n))/3))))
    return '$%.2f%s'%(n/10**(3*millidx),millnames[millidx])

def donation_verb(s):
    verbs = {
        '1A': 'donated',
        '2A': 'transferred',
        '3A': 'loaned',
        '4A': 'gave',
        '5A': 'donated inkind'
    }
    return verbs.get(s, 'donated')

def donation_name(s):
    verbs = {
        '1A': 'Donation',
        '2A': 'Transfer in',
        '3A': 'Loan received',
        '4A': 'Other',
        '5A': 'In kind donation'
    }
    return verbs.get(s, 'donated')

def expense_verb(s):
    verbs = {
        '6B': 'transferred',
        '7B': 'loaned',
        '8B': 'spent',
        '9B': 'spent',
    }
    return verbs.get(s, 'spent')

def expense_name(s):
    verbs = {
        '6B': 'Transfer out',
        '7B': 'Loan made',
        '8B': 'Expenditure',
        '9B': 'Independent Expenditure',
    }
    return verbs.get(s, 'spent')

def contested_races_description(s):
  if s == "House of Representatives":
    description = "Contested 2016 General Election Races for the Illinois State House of Representatives in all districts. Click on a row to see detailed information."
  elif s == "Senate":
    description = "Contested 2016 General Election Races for the Illinois Senate in all districts. Click on a row to see detailed information."
  elif s == "State Comptroller":
    description = "Contested 2016 Race for Illinois State Comptroller. Click on a row to see detailed information."
  else:
    description = ""

  return description

def committee_description(s):
  if s == "Candidate":
    description = "Candidate committees accept campaign contributions and make expenditures under the candidate's authority in order to further their bid for election or re-election to public office. They are subject to state and federal contribution limits."
  elif s == "Super PAC":
    description = "Super PACs, known as Independent Expenditure Committees in the Illinois Election Code, may raise unlimited sums of money from corporations, unions, associations and individuals, then spend unlimited sums to overtly advocate for or against political candidates or issues. Unlike traditional PACs, independent expenditure committees are prohibited from donating money directly to political candidates."
  elif s == "Political Action":
    description = "A political action committee (PAC) is a type of organization that gathers campaign contributions from members and spends those funds to support or oppose candidates, ballot initiatives, or legislation. These committees are subject to state and federal contribution limits."
  elif s == "Political Party":
    description = "A political party committee is an organization, officially affiliated with a political party, which raises and spends money to support candidates of that party or oppose candidates of other parties. These committees are subject to some contributions limits, and funds are often transferred from Political Party Committees to Candidate Committees."
  elif s == "Ballot Initiative":
    description = "A ballot initiative is created by a petition signed by a minimum number of registered voters to bring about a public vote on a proposed statute or constitutional amendment. A group in support or opposition of this type of public policy is considered to be a ballot initiative committee. These committees are not subject to contributions limits."
  else:
    description = ""

  return description

def format_number(s):
    return '{:,}'.format(s)

def format_large_number(n):
    import math
    millnames=['','Thousand','Million','Billion','Trillion']
    n = float(n)
    millidx=max(0,min(len(millnames)-1,
                      int(math.floor(math.log10(abs(n))/3))))
    return '%.1f %s'%(n/10**(3*millidx),millnames[millidx])

def slugify(text):
    import re
    delim = '-'

    if text:
        punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:;]+')
        result = []
        for word in punct_re.split(text.lower()):
            if word:
                result.append(str(word))
        return delim.join(result)
    else: # pragma: no cover
        return text
