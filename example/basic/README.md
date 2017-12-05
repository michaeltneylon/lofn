# Word Count

Using lofn and bash to do a distributed word count. Each map runs bash commands to count unique words.
Each partition results in a word count list that is then added to a running total in the reduce
step until complete, and the total word counts are returned.

## Data

You can run this example wordcount on any file, we used the most downloaded book on
[Project Gutenberg](https://www.gutenberg.org/):

- [Pride and Prejudice by Jane Austen](https://www.gutenberg.org/files/1342/1342-0.txt)

## Usage

```
usage: spark-submit word_count.py [-h] [-o OUTPUT] input_file

Word Count with lofn

positional arguments:
  input_file            An input text file from which to count words

optional arguments:
  -h, --help            show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output file name. Default is 'word_counts.txt
```

To use Pride and Prejudice in this example on standalone Spark:

```
wget https://www.gutenberg.org/files/1342/1342-0.txt
spark-submit word_count.py 1342-0.txt
```

## Output

A 'word_counts.txt' file with all the unique words and their frequency.

The script will also print out the most frequent word(s). In our example using Pride and Prejudice,
that output results in:

`Most frequent word(s) occurring 4205 times: the`

## Run on YARN

To run this on YARN, the input data must reside on HDFS and paths provided must be absolute paths on HDFS.
