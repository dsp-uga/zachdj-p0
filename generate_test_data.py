from math import floor
import random

"""
This script writes text files with well-known word counts for the purpose of testing.
The files are written to the testdata directory

The text files include n repetitions of every integer n between 1 and 50, expressed as a word.
The numbers are converted to 'words' by repeating the unit word for each decimal place
For example, 40 becomes "fourzero", 3 becomes "three", 25 becomes "twofive", etc.
"""

digit_words = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine']
punctuation = ['.', ',', ':', ';', '\'', '`' '!', '?']


def int_to_word(n, capitalize=False, punctuate=False):
    """
    Takes an integer and converts it into a "word" with English letters
    Optionally capitalizes part of the word or adds random punctuation (, or . or ! or ?) to the end of the word

    :param n: the integer to convert to a word
    :param capitalize: if True, capitalizes the word
    :param punctuate: if True, adds a random punctuation mark to the beginning or end of the word
    :return: the given integer converted to a word
    """
    word = ""
    while n > 0:
        digit = n % 10  # gets the digit in the ones place
        word = digit_words[digit] + word  # prepend the digit to the word
        n = floor(n / 10)  # move to the next digit
    if capitalize:
        word = word.capitalize()
    if punctuate:
        punctuation_mark = random.choice(punctuation)
        prefix = random.choice([True, False])
        if prefix:
            word = punctuation_mark + word
        else:  # suffix if not prefix
            word = word + punctuation_mark

    return word

# write some words with no punctuation
with open('testdata/clean.txt', 'w') as file:
    for num in list(range(1, 51)):
        for i in list(range(0, num)):
            word = int_to_word(num, capitalize=False, punctuate=False) + " "
            file.write(word)  # repeat the word num times
        file.write("\n")  # separate words onto different lines

    # write some words filled with capitalization and punctuation!
    for num in list(range(1, 51)):
        for i in list(range(0, num)):
            cap = random.random() < 0.25  # capitalize 25% of the words (25% chosen arbitrarily; we just want some)
            punc = random.random() < 0.10  # add punctuation to 10% of the words
            word = int_to_word(num, capitalize=cap, punctuate=punc) + " "
            file.write(word)  # repeat the word num times
        file.write("\n")  # separate words onto different lines
