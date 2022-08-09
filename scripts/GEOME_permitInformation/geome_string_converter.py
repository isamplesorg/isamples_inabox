import re

# convert authorizedBy array to map object
def formatResultObject(authorizedBy):
    return {"authorizedBy": authorizedBy, "compliesWith": []}

def splitString(string):
    noData = ['nan', 'na', 'no data', 'unknown', 'none_required']
    originalString = str(string)

    # If the string is NA
    if(originalString.lower() in noData): return formatResultObject([])

    # Remove quotes
    originalString = re.sub(r'\"', "", originalString)

    
    slash_N = len(re.findall(r'/', originalString))
    comma_N = len(re.findall(r'\,', originalString))

    # e.g. DAFF/DEA
    if(slash_N == 1 and originalString.replace(" ", "") == originalString):
        return formatResultObject(originalString.split("/"))

    # If there are multiple slash "/", we need to split string by " and " or ", "
    if(slash_N > 1 and not comma_N > 1):
        # replace typo error "/ " to "/"
        originalString = re.sub("/ ", "/", originalString)

        if(re.findall(' and ', originalString)):
            return formatResultObject(originalString.split(" and ")) 
        else:
            return formatResultObject(originalString.split(", ")) 
    
    # If there are multiple commas, ", ", we need to split string by ", " and remove "&" or "and"
    if(comma_N > 1 and (", and " in originalString or ", & " in originalString) and not slash_N > 1):
        # Ignore long string
        if(len(re.findall('and', originalString)) > 1):
            return formatResultObject([originalString]) 

        if(" and " in originalString):
            originalString = originalString.replace(" and ", ' ')
        
        if(" & " in originalString):
            originalString = originalString.replace(" & ", ' ')

        return formatResultObject(originalString.split(", "))
    
    # Split string by semicolon, ";" but ignore long string with multiple separator
    if (re.findall("; ", originalString) and not comma_N > 1):
        return  formatResultObject(originalString.split("; ")) 

    return formatResultObject([originalString]) 