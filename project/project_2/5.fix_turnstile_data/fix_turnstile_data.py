import csv
import pandas as pd

def fix_turnstile_data(filenames):
    '''
    Filenames is a list of MTA Subway turnstile text files. A link to an example
    MTA Subway turnstile text file can be seen at the URL below:
    http://web.mta.info/developers/data/nyct/turnstile/turnstile_110507.txt
    
    As you can see, there are numerous data points included in each row of the
    a MTA Subway turnstile text file. 

    You want to write a function that will update each row in the text
    file so there is only one entry per row. A few examples below:
    A002,R051,02-00-00,05-28-11,00:00:00,REGULAR,003178521,001100739
    A002,R051,02-00-00,05-28-11,04:00:00,REGULAR,003178541,001100746
    A002,R051,02-00-00,05-28-11,08:00:00,REGULAR,003178559,001100775
    
    Write the updates to a different text file in the format of "updated_" + filename.
    For example:
        1) if you read in a text file called "turnstile_110521.txt"
        2) you should write the updated data to "updated_turnstile_110521.txt"

    The order of the fields should be preserved. 
    
    You can see a sample of the turnstile text file that's passed into this function
    and the the corresponding updated file in the links below:
    
    Sample input file:
    https://www.dropbox.com/s/mpin5zv4hgrx244/turnstile_110528.txt
    Sample updated file:
    https://www.dropbox.com/s/074xbgio4c39b7h/solution_turnstile_110528.txt
    '''
    for name in filenames:
        in = open(name, 'rb')
        out = open('updated_'+name, 'wb')

        input = csv.reader(in, delimiter = ',')
        ouput = csv.writer(out)

        for line in input:
            c1 = line[0]
            c2 = line[1]
            c3 = line[2]

            i = 3

            while i < len(line):
                line_updated = [c1, c2, c3, line[i], line[i+1], line[i+2], line[i+3], line[i+4]]
                writer_out.writerow(line_updated)
                i = i+5

        

if __name__ == "__main__":
    input_files = ['turnstile_110528.txt', 'turnstile_110604.txt']
    fix_turnstile_data(input_files)
