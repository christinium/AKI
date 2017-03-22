import datetime
import re 

#re.sub(r'\sAND\s', ' & ', 'Baked Beans And Spam', flags=re.IGNORECASE)
date = re.sub(r"[:|\s|\.]","-",str(datetime.datetime.now()))
print date
