## communitize

### What this does

Runs a distributed computation of the cosine distance of the item-vecotrs of a user-item matrix

### How


### Input

An user-item matrix:

           iPad  iPhone  VW
    Nate     -      5     3
    Jay      4      3     - 

The rating for the user must be between 1 (dislike) - 5 (like). Leave out a row which is "unrated" (-) 

File format:

    Nate,iPhone,5
    Nate,VW,5
    Jay,iPad,4
    Jay,iPhone,3

### Output

Item-to-item similarity

    xxx

### Credit

* Originally based on outline of ItemSimilarityJob in the Apache Mahout project. 
* Algorithm used is a slight modification from the algorithm described in 
  http://www.umiacs.umd.edu/~jimmylin/publications/Elsayed_etal_ACL2008_short.pdf
* HadoopInterop.scala forked from jmhodges "componentize" 

