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

    # (userId, itemId, prefVal)
    Nate,iPhone,5
    Nate,VW,5
    Jay,iPad,4
    Jay,iPhone,3

Phase 1: Create the item-vectors
  
    # (itemId, userId, prefVal)
    VW      Nate,5
    iPad    Jay,4
    iPhone  Nate,5|Jay,3

Phase 2: Compute the length of the item vectors, store it with the item, create the user-vectors 

    # (userId, (itemId, ivLength, prefVal) ...)
    Jay     iPad,4.0,4.0|iPhone,5.830951894845301,3.0
    Nate    VW,5.0,5.0|iPhone,5.830951894845301,5.0


### Output

Item-to-item similarity

    xxx

### Credit

* Outline originally based on ItemSimilarityJob in the Apache Mahout project. 
* Algorithm used is a slight modification from the algorithm described in 
  http://www.umiacs.umd.edu/~jimmylin/publications/Elsayed_etal_ACL2008_short.pdf
* HadoopInterop.scala forked from jmhodges "componentize" 
* and a bit from: http://blog.jonhnnyweslley.net/2008/05/shadoop.html

