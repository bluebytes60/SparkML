# SparkML
This is a repo for some spark ML trials

## Feature Selection - Chi-square function
Chi-square function is a nice supervised feature selection technique. A chi-square function measures the relevance between a term t  and a category c. For example, we can measure the relevance of a word "water" and a category "weather". The total weight of t is the sum of its chi-square values over different categories (c1, c2, c3, ... cn). The formula is as follows:

chi(t, c) = N * (a*d-b*c)^2/((a+b)(c+d)(a+c)(b+d)),
N is total document frequency of every term,
a is the document frequency of t in c,
b is the document frequency of other term (~t) in c,
c is the document frequency of t in other categories (~c),
d is the document frequency of ~t in ~c.

1. The code use spark for chi-square value calculation.
2. The code should be scalable in muilple machine
