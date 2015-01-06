width = 80


def printStars():
  print("".join([ '*' for i in range(width)]))

def printDashes():
  print("".join([ '-' for i in range(width)]))


def logHeader(s):
  printStars()
  print(s)
  printStars()
  
def logEvent(indent, s):
  tabs = "".join(['\t' for i in range(indent)])
  print("%s%s" % (tabs, s)) 

def endSection():
  printDashes()

