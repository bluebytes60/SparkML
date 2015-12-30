# sparkExcerise
This is a repo for some spark trials

## Feature Selection - Chi-squared function
This is chi-square function calculation does not employ the negative discriminating power but only positive discriminating power.
In other words, for a category, only positive features are selected and calculated. Because positive features are more easy to understand and to interpret.
However, the best performance will not be achieve if both positive and negative features are utilized.

1. The code try to use spark for chi-square value calculation.
2. The code is not fully follow streaming and counting model and it's not stateless so global memory cache is still used.
3. Future work may working on using Redis distributed memory cache or better(less io, less memory usage) computing method.

