import sys

# pathway separator
# Choose seperator type based on whether or not
# system is Linux or Windows
chk_os = platform.system()
if chk_os == "Windows":
    sep = "\\"
else:
    sep = "/"
print("System is: {}\nSeparator is: {}".format(chk_os, sep))
