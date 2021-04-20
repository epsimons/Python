#!/usr/bin/env python3

# This will read an input list in groups of 3. If one of the groupings does not have 3
# items, just add some text to ensure compatibility.
# this can be called from an external script with:
#
# import make_html
#
# make_html.main( [ "row 1, col 1", "row 1, col 2", "row 1, col 3", "row 2, col 1", "row 2, col 2", "row 2, col 3" ] )
#   -- or --
# make_html.main( my_list )
#
# either way will work.


import os

TABLE_CONTENT_LIST = []

# initialize basic components of HTML page and table structure

# HTML_FILE_NAME = "comparison_table.html" # changed to a variable to pass

# change style to suit your needs
# Standard CSS3

HTML_Beginning = """
<!DOCTYPE html>
<html>
<head>
<style>
html {
font-size: 1em;
font-weight: normal;
letter-spacing: 1px;
color: rgb(0,0,0);
background-color: rgb(250,250,250);
font-family: "Times New Roman", Times, serif;
}
table {
border-collapse: collapse;
margin-left: auto;
margin-right: auto;
}
table, th, td {
border: 1px solid rgb(0, 0, 0);
border-collapse: collapse;
font-size: 10pt;
}
th {
text-align: center;
background-color: rgb(5, 5, 235);
color: rgba(250,250,250,1);
font-size: 1.2em;
font-weight: bold;
width: 25%;
}
td {
text-align: left;
}
th, td {
padding: 10px;
}
tr {
	background-color: rgb(250,250,250);
}
tr:nth-child(even) {
	background-color: rgba(190, 190, 190, 1);
}

tr:hover {
background-color: rgba(50,50,50,1);
color: rgba(250,250,250,1);
}
table thead tr{
    display:block;
}

table thead tr {
    display: block;
}

table th, table td {
    width: 350px;
}

table tbody{
  display:block;
  height:600px;
  overflow:auto;
}
.red {
    background-color: rgb(255, 0, 0);
    color: rgb(233, 233, 15);
}
</style>
<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.4.1/jquery.min.js"></script>
</head>
<body>
<p>
<br/>
<small>NOTE: Some files may take a long time to load.</small>
<br/>
</p>
<table>
<thead>
<tr>
<th>File From XML List</th>
<th>File Location in Mirror (data1)</th>
<th>File Location in Media (storage)</th>
<th>File Location in data2 (temp storage)</th>
</tr>
</thead>
<tbody>
            """


HTML_Ending = """
</tbody>
</table>

<script>
$("td:contains('missing')").closest("tr").css({"background-color":"#ff0000","color":"yellow"});
</script>
</body>
</html>
              """

def main(HTML_FILE_NAME, TABLE_CONTENT_LIST=[]):
    # open HTML file for writing
    try:

        if os.path.isdir("html_logs/"):
            pass
        else:
            os.mkdir("html_logs/")

        my_file = "html_logs/{}".format(HTML_FILE_NAME)
        f = open(my_file, "w")

        f.write(HTML_Beginning)

        #print(length_of_list) # debugging
        #print(TABLE_CONTENT_LIST) # debugging
        if len(TABLE_CONTENT_LIST)%4 == 0:
                pass
        elif len(TABLE_CONTENT_LIST)%4 == 1:
            TABLE_CONTENT_LIST.append("N/A")
            TABLE_CONTENT_LIST.append("N/A")
            TABLE_CONTENT_LIST.append("N/A")
        elif len(TABLE_CONTENT_LIST)%4 == 2:
            TABLE_CONTENT_LIST.append("N/A")
            TABLE_CONTENT_LIST.append("N/A")
        elif len(TABLE_CONTENT_LIST)%4 == 3:
            TABLE_CONTENT_LIST.append("N/A")
        # iterate through the list to build the table
        temp_var = 0
        for i in TABLE_CONTENT_LIST[::4]:
            f.write("\n<tr>\n<td>{}</td>\n".format(TABLE_CONTENT_LIST[temp_var]))
            temp_var = temp_var + 1
            f.write("<td>{}</td>\n".format(TABLE_CONTENT_LIST[temp_var]))
            temp_var = temp_var + 1
            f.write("<td>{}</td>\n".format(TABLE_CONTENT_LIST[temp_var]))
            temp_var = temp_var + 1
            f.write("<td>{}</td>\n</tr>\n".format(TABLE_CONTENT_LIST[temp_var]))
            temp_var = temp_var + 1
            #print(i[0],i[1],i[2]) # debugging
        #print(TABLE_CONTENT_LIST[:temp_var:]) # debugging
        #print(HTML_Ending) # debugging
        f.write(HTML_Ending)
        f.close()
    except Exception as e:
        print(e.args)
        f.write("</table><br><h1>ERROR</h1></body></html>")
        f.close()


if __name__=="__main__":
    # can be run as a demo, or pass in a list as
    # explained above
    TABLE_CONTENT_LIST = ["File Listed in XML", "/path/to/file_on_mirror","/path/to/file_on_data1", "/path/to/file_on_temp1",
                          "File Listed in XML2", "/path/to/file_on_mirror2","/path/to/file_on_data2", "/path/to/file_on_temp2",
                          "File Listed in XML3", "/path/to/file_on_mirror3","/path/to/file_on_data3", "/path/to/file_on_temp3",
                          "File Listed in XML4", "/path/to/file_on_mirror4","missing", "/path/to/file_on_temp1",
                          "File Listed in XML5", "/path/to/file_on_mirror5","/path/to/file_on_data5", "/path/to/file_on_temp4",
                          "Incomplete"
                          ] # test data

    main("Test_Table.html",TABLE_CONTENT_LIST)








