# sparkExcerise
This is a repo for some spark trials

## Feature Selection - Chi-squared function
This chi-square function calculation does not employ the negative discriminating power but only positive discriminating power.
In other words, for a category, only positive features are selected and calculated. Because positive features are more easy to understand and to interpret for many useage.
However, the best result(ex: Text Categorization) will not be achieve if both positive and negative features are not utilized.

1. The code try to use spark for chi-square value calculation.
2. The code is not fully follow streaming and counting model and it's not stateless so global memory cache is still used.
3. Future work may working on using Redis distributed memory cache or better(less io, less memory usage) computing method.

Note that, the training data is obtained from internet, and it's already preprocessed, so the chi-square result will not show it's best quality. But it should work better with raw dataset.

Code is disaster, will neet to improve this shit
