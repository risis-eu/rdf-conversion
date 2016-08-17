# coding=utf-8
import re
import os
import codecs
import cStringIO
import datetime


__name__ = """WriteRDF"""


# RUN Orgref from MY LAPTOP
# cd \.
# cd C:\Users\Al\Apps\PythonApps\LinkTo\Utility
# python
# import WriteRDF
#
# sep = ','
# col_id = 10
# data_set = "OrgRef"
# to_convert = "C:\System\PhD-Sys\EKAW Data\orgref20160106\orgref.csv"
#
# WriteRDF.run(file_to_convert=to_convert, separator=sep, subject_id=col_id, database=data_set,  is_trig=True)
#
# reload(WriteRDF)


def run(file_to_convert, separator, subject_id, database, is_trig):

    bom = ''

    # Open the file to convert
    orgref_file = open(file_to_convert)

    # get the first line
    first_line = orgref_file.readline().strip('\r\n')

    # Check if it contains BOM. If yes, remove BOM
    if first_line.startswith(codecs.BOM_UTF8):
        print ''
        print "This file starts with BOM"
        for i in range(len(codecs.BOM_UTF8)):
            bom += first_line[i]
        first_line = first_line.replace(bom, '')

    # Get the attribute headers
    header = WriteRDF.extractor(first_line, separator)

    # print header

    rdf_writer = WriteRDF(file_to_convert, header, subject_id, database, is_trig)

    n = 0
    while True:
        n += 1
        line = orgref_file.readline()

        if not line:
            print '\nNo more line... Process ended at line > ' + str(n) + "\n"
            print 'Done with converting [' + file_to_convert + '] to RDF!!!'
            orgref_file.close()
            break

        # if n == 5:
        #     print line
        # if n == 6:
        #     rdf_writer.close_writer()
        #     break

        rdf_writer.write_triples(line, separator)

    if rdf_writer.isClosed is not True:
        rdf_writer.close_writer()


class WriteRDF(object):

    """ Constructor
    input_path   -> string   denoting the out file path
    csv_header   -> Array    denoting the list of attributes in the csv file
    subject_id   -> int      denoting the index of the attribe to use as identification
    database     -> string   denoting the database name
    is_trig      -> boolean  checks wether the turtle file is a trig format"""
    def __init__(self, input_path, csv_header, subject_id, database, is_trig):

        self.isTrig = is_trig         # -> boolean  Checks the RDF format of the output file
        self.inputPath = input_path   # -> string   The out file path
        self.csvHeader = csv_header   # -> Array    The list of attributes in the csv file
        self.subjectID = subject_id   # -> int      The index of the attribe to use as identification
        self.database = database      # -> string   The database name
        self.longestHeader = 0        # -> int      The number of characters in the longest attribute
        self.lastColumn = 0           # -> int      The last attribute index
        self.fileName = ""            # -> string   The name of the input file to set the output file name
        self.pvFormat = ""            # -> string   Representing RDF triple format to use for formatting Predicate_Value
        self.fileUpdate = 1
        self.refreshCount = 0
        self.instanceCount = 0
        self.fileSplitSize = 40000
        self.schema = "schema:"
        self.vocab = "vocab:"
        self.outputExtension = "trig"
        self.risisOntNeutr = "riClass:Neutral"
        self.date = datetime.date.isoformat(datetime.date.today())
        self.isClosed = False
        self.database = self.database.strip().replace(" ", "_")
        self.nameSpace = self.get_name_space(self.database)

        ''' 1. Set the output file path
        os.path.splitext(inputPath)[0] separate C:\kitty.jpg.zip as [C:\kitty.jpg] and [.zip]'''
        self.fileName = os.path.basename(os.path.splitext(self.inputPath)[0])
        self.fileNameWithExtension = self.fileName + "." + str(self.fileUpdate) + "." + \
                                     str(self.date) + "." + self.outputExtension
        self.outputPath = re.sub(os.path.basename(self.inputPath), self.fileNameWithExtension, self.inputPath)

        print ""
        print "Output file > " + self.outputPath

        """ 2. Get the last column ID. This allows to stop the loop before the end
        whenever the identification column happens to be the last column"""
        self.lastColumn = len(self.csvHeader) - 1
        if self.subjectID == self.lastColumn:
            self.lastColumn -= 1

        """ 3. Get the attribute headers and make them URI ready"""
        for i in range(0, len(self.csvHeader)):

            '''Replace unwanted characters -> #;:.-(–)—[']`=’/”{“}^@*+!~\,%'''
            pattern = r'[#;:%!~+`=’*.(\-)–\\—@\['',\\]`{^}“/”]'
            self.csvHeader[i] = re.sub(pattern, "", self.csvHeader[i])

            '''For every attribute composed of more than 1 word and separated by space,
            stat the first word with lower case followed by the underscore character'''

            # print self.csvHeader

            header_split = self.csvHeader[i].split()
            new_header = header_split[0].lower()
            for j in range(1, len(header_split)):
                new_header += "_" + header_split[j]
                self.csvHeader[i] = new_header
            # print header_split

            '''Get the size (number of characters) of the longest attribute'''
            if self.longestHeader < len(self.csvHeader[i]):
                self.longestHeader = len(self.csvHeader[i])

        """ 4. Set the RDF triple formatter """
        sub_position = 6
        # vocab: takes 6 slots
        pre_position = sub_position + self.longestHeader
        self.pvFormat = "{0:>" + str(sub_position) + "} {1:" + str(pre_position) + "} {2}"

        """ 5. Set the writer and write in the file the RDF namespaces """
        self.writer = open(self.outputPath, "w+")
        self.writer.write(self.nameSpace)

        """ 6. Write in the implicit schema """
        self.get_schema()

        if self.isTrig is True:
            self.open_trig()

        """ Regular Expressions that might me needed for checking predicate values """
        self.rgxInteger = re.compile("^[-+]?\d+$", re.IGNORECASE)
        self.rgxDecimal = re.compile("^[-+]?\d*[.,]\d+$", re.IGNORECASE)
        self.rgxDouble = re.compile("^[-+]?\d*[.,]?\d+[eE][-+]?[0-9]+$", re.IGNORECASE)
        self.rgxDate = re.compile("^\d{4}[-/.](0[1-9]|1[012])" +
                                  "[- /.](0[1-9]|[12][0-9]|3[01])([zZ]?|[-+](0[1-9]|1[0-9]|2[0-4]):00)$", re.IGNORECASE)

        self.rgxTime = re.compile("^(0[1-9]|1[0-9]|2[0-3]):[0-5][0-9]" +
                                  "(:[0-5][0-9](.[0-9]?[0-9]?[0-9]?)?)" +
                                  "([zZ]|[-+](0[1-9]|1[0-9]|2[0-4]):00)?$", re.IGNORECASE)

        self.rgxDateTime = re.compile("^\\d{4}[-/.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])([zZ]?|[-+](0[1-9]" +
                                      "|1[0-9]|2[0-4]):00)T(0[1-9]|1[0-9]|2[0-3]):[0-5][0-9](:[0-5][0-9]" +
                                      "(\\.[0-9]?[0-9]?[0-9]?)?)([zZ]|[-+](0[1-9]|1[0-9]|2[0-4]):00)?$", re.IGNORECASE)

    @staticmethod
    def get_name_space(database):
        """ This function outputs the static hardcoded namespace required for the OrgRef dataset """
        name_space = cStringIO.StringIO()
        name_space.write("\t### Name Space #########################################################################\n")
        name_space.write("\t@base <http://risis.eu/> .\n")
        name_space.write("\t@prefix dataset:<dataset#> .\n")
        name_space.write("\t@prefix schema:<vocabulary#> .\n")
        name_space.write("\t@prefix data:<resource/{0}#> .\n".format(database))
        name_space.write("\t@prefix vocab:<resource/{0}/schema#> .\n".format(database))
        name_space.write("\t@prefix riClass:<ontology/classes#> .\n")
        name_space.write("\t@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        name_space.write("\t@prefix xsd:<http://www.w3.org/2001/XMLSchema#> .\n")
        name_space.write("\t@prefix owl: <http://www.w3.org/2002/07/owl#> .\n")
        name_space.write("\t########################################################################################\n")
        content = name_space.getvalue()
        name_space.close()
        return content

    def get_schema(self):
        """ This function gets the set of attribute header as the NEUTRAL implicit Orgref RDF schema """
        self.write_line("")

        if self.isTrig is True:
            self.write_line("### [ About the schema of " + str(self.database) + " ]")
            self.write_line(self.schema + self.database.strip().replace(" ", "_"))
            self.write_line("{")

        for i in range(0, len(self.csvHeader)):
            self.write_line("\t### [ " + str(i + 1) + " About the attribute: \"" + self.csvHeader[i] + "\" ]")
            self.write_line("\t" + self.vocab + self.csvHeader[i].strip().replace(" ", "_"))
            self.write_line(self.pvFormat.format("", "rdf:type", self.risisOntNeutr + " ,"))
            self.write_line(self.pvFormat.format("", "", "rdf:Property" + " ."))
            if i != len(self.csvHeader) - 1:
                self.write_line("")

        if self.isTrig is True:
            self.write_line("}")

    def open_trig(self):
        """ This function sets the name of the named-graph and opens the curly bracket"""
        self.write_line("")
        self.write_line("### [ About " + str(self.database) + " ]")
        self.write_line("dataset:" + self.database)
        self.write_line("{")

    def close_trig(self):
        """ This function closes the named-graph """
        self.writer.write("}\n")

    def close_writer(self):
        """ This function closes the dataset named-graph and the writer  """
        if self.isTrig is True:
            self.close_trig()

        self.writer.close()
        self.isClosed = True

    def write_line(self, string):
        """ This function allows writing new line to a file without continuously adding the newline escape character """
        self.writer.write(string + '\n')

    def triple_value(self, value):
        """ This function takes as input the predicate's value and returns it in the write format, be it a
            integer, decimal, double, boolean, date, time or dateTime datatype or whether it is a URI """

        # Check whether the value is null or empty
        if value is None == "True":
            value = value.trim()

        # Return an empty string if the value is an empty string
        if value == "":
            return ""

        # Replace double quote with a single quote
        value = str(value.strip()).replace("\"", "'")

        # URI values
        if ("http://" in value or "https://" in value) and " " not in value:
            return "<" + value + ">"

        # NUMBERS: can be written like other literals with lexical form and datatype
        elif self.rgxInteger.match(value):
            return "\"" + value + "\"^^xsd:integer"

        elif self.rgxDecimal.match(value):
            return "\"" + value + "\"^^xsd:decimal"

        elif self.rgxDouble.match(value):
            return "\"" + value + "\"^^xsd:double"

        # BOOLEAN: values may be written as either 'true' or 'false' (case-sensitive)
        # and represent RDF literals with the datatype xsd:boolean. """
        elif value.lower() == "true" or value.lower() == "false":
            return "\"" + value + "\"^^xsd:boolean"

        # DATE: specified in the following form "YYYY-MM-DD"
        # Note: All components are required!
        elif self.rgxDate.match(value):
            return "\"" + value + "\"^^xsd:date"

        # TIME:
        elif self.rgxTime.match(value):
            return "\"" + value + "\"^^xsd:time"

        # DATE - TIME:
        elif self.rgxDateTime.match(value):
            return "\"" + value + "\"^^xsd:dateTime"

        # TEXT \u005c
        elif re.search("[“”’`\r\n'\"]+", value, re.IGNORECASE):
            return "\"\"\"" + value + "\"\"\""

        else:
            return "\"" + value + "\""

    def write_subject(self, subject_resource):
        """ This function takes in a string value as subject resource and returns a formatted resource
        If the file conatins more triples that self.fileSplitSize a new file is creates. Also as meta
        data, the sunber of triples is counted """
        if subject_resource is not None:
            subject_resource = subject_resource.strip()

        if subject_resource != "":
            self.refreshCount += 1

        if self.refreshCount > self.fileSplitSize:
            self.refresh()
            self.refreshCount = 0

        self.instanceCount += 1
        self.write_line("\t### [ " + str(self.instanceCount) + " ]")
        self.write_line("\tdata:" + subject_resource.strip().replace(" ", "_"))

    def write_record_values(self, record):
        """ This function takes as an argument a csv record as an array witch represents a csv line in the dataset """
        # if record is not None:
        #   record = record.strip()

        # print str(len(record))
        # print record
        for i in range(0, len(self.csvHeader)):
            cur_value = record[i].strip()
            # print str(i) + " " + self.csvHeader[i] + "\t" + cur_value

            if i != self.subjectID:

                # The last column has a value so, end the triple with a dot
                if i == self.lastColumn and cur_value != "":
                    self.write_line(self.pvFormat.format("", self.vocab + self.csvHeader[i],
                                                         self.triple_value(cur_value)) + " .")
                    self.write_line("")

                # The last column does not have a value => No triple but end of the subject.
                elif i == self.lastColumn and cur_value == "":
                    self.write_line("{0:>6}".format("."))
                    self.write_line("")

                # Normal business
                elif cur_value != "":
                    # print("\n" + "[" + str(i) + "]" + self.csvHeader[i] + ": " + cur_value)
                    self.write_line(self.pvFormat.format("", self.vocab + self.csvHeader[i],
                                                         self.triple_value(cur_value)) + " ;")
        # print ""

    def write_triples(self, line, separator):

        record = self.extractor(line, separator)
        subject_resource = record[self.subjectID]

        if subject_resource is not None:
            subject_resource = subject_resource.strip()
        if subject_resource != "":
            self.refreshCount += 1
        if self.refreshCount > self.fileSplitSize:
            self.refresh()
            self.refreshCount = 0

        # Write the subject
        self.instanceCount += 1
        self.write_line("\t### [ " + str(self.instanceCount) + " ]")
        self.write_line("\tdata:" + subject_resource.strip().replace(" ", "_"))

        # Write the values
        self.write_record_values(record)
        # print record

    def refresh(self):
        """ This funtion makes sure that a new file is created whenever self.fileSplitSize is reached """

        self.close_trig()
        self.writer.close()
        self.isClosed = True

        self.fileUpdate += 1
        self.outputPath = self.outputPath.replace(self.fileNameWithExtension, self.fileName + "." +
                                                  str(self.fileUpdate) + "." + str(self.date) + "." +
                                                  self.outputExtension)

        print ""
        print "Output file > " + self.outputPath

        self.writer = open(self.outputPath, "w+")
        self.write_line(self.nameSpace)

        """ 6. Write in the implicit schema """
        self.get_schema()

        if self.isTrig is True:
            self.open_trig()

        self.isClosed = False

    def set_file_split_size(self, value):
        self.fileSplitSize = value

    @staticmethod
    def extractor(record, separator):
        td = '"'
        attributes = []
        temp = ""

        # print record
        i = 0
        while i < len(record):

            if record[i] == td:
                j = i + 1
                while j < len(record):
                    if record[j] != td:
                        temp += record[j]
                    elif j + 1 < len(record) and record[j + 1] != separator:
                        if record[j] != td:
                            temp += record[j]
                    elif j + 1 < len(record) and record[j+1] == separator:
                        j += 2
                        break
                    j += 1

                attributes.append(temp)
                temp = ""
                i = j

            else:
                while i < len(record):

                    # Enqueue if you encounter the separator
                    if record[i] == separator:
                        attributes.append(temp)
                        # print "> separator " + temp
                        temp = ""

                    # Append if the current character is not a separator
                    if record[i] != separator:
                        temp += record[i]
                        # print "> temp " + temp

                    # Not an interesting case. Just get oit :-)
                    else:
                        i += 1
                        break

                    # Increment the iterator
                    i += 1

        # Append the last attribute
        if temp != "":
            attributes.append(temp)

        return attributes


try:

    # run(file_to_convert=input("File to convert "), separator=input("Separator "),
    #     subject_id=input("column ID "), database=input("Dataset name "),  is_trig=True)

    # ETER
    eter_sep = ';'
    eter_id = 1
    eter_set = "eter"
    eter_file = "C:\System\PhD-Sys\EKAW Data\RDF\eter_export_.csv"
    run(file_to_convert=eter_file, separator=eter_sep, subject_id=eter_id, database=eter_set, is_trig=True)
    #
    # OrgRef
    # orgref_sep = ','
    # orgref_id = 10
    # orgref_set = "orgref"
    # orgref_file = "C:\System\PhD-Sys\EKAW Data\RDF\orgref.csv"
    # run(file_to_convert=orgref_file, separator=orgref_sep, subject_id=orgref_id, database=orgref_set, is_trig=True)

    # GRID PLACE
    # grid_place_sep = ','
    # grid_place_id = 0
    # grid_place_data = 'grid place'
    # grid_place_file = "C:\System\PhD-Sys\EKAW Data\RDF\grid.csv"
    # run(file_to_convert=grid_place_file, separator=grid_place_sep,
    #     subject_id=grid_place_id, database=grid_place_data, is_trig=True)

except ValueError:
    print "A problem ha occurred.\nUnable to run the code with the input provided.\nTry again"

#
# file = open(to_convert, 'r+')
# line = file.readline()
#
# print WriteRDF.extractor(line, sep)
