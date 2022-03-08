#include <math.h>
#include <stdlib.h>
#include <stdio.h>

/**
 * This was done based on the impl below
 *
 * https://www.csee.usf.edu/~kchriste/tools/genzipf.c
 */

struct ZipfGenerator {
  double c;         // Normalization constant
  int n;            // Number of elements
  double theta;     // Zipfian theta parameter
  double *sumProb;  // pre calculate the sum probabilities
};

struct ZipfGenerator* ZipfGeneratorNew(double theta, int nElements) {
  struct ZipfGenerator *p = (struct ZipfGenerator*)malloc(sizeof(struct ZipfGenerator));
  double c = 0;
  for(int i = 1; i <= nElements; ++i) {
    c += 1.0 / pow((double) i, theta);
  }
  p->c = 1.0 / c;
  p->n = nElements;
  p->theta = theta;
  p->sumProb = (double*)calloc(nElements+1, sizeof(double));
  p->sumProb[0] = 0;
  for(int i = 1; i <= nElements; ++i) {
    p->sumProb[i] = p->sumProb[i-1] + p->c / pow((double) i, theta);
  }
  return p;
}

int ZipfGenerate(struct ZipfGenerator *p) {
  double z;

  // Pull a uniform random number (0 < z < 1)
  do {
    z = drand48();
  } while ((z == 0) || (z == 1));

  // Map z to the value
  int low = 1, high = p->n, mid;
  while (low <= high) {
    mid = (low + high) / 2;
    if (p->sumProb[mid] >= z) {
      high = mid - 1;
    } else {
      low = mid + 1;
    }
  }
  return low;
}
