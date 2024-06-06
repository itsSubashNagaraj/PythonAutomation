import xml.etree.ElementTree as ET
mytree = ET.parse('report.xml')
myroot = mytree.getroot()
print(myroot)

for ch in myroot:  
    print(ch.tag, ch.attrib)  

