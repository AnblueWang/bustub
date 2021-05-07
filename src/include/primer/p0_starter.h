//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include "common/logger.h"

namespace bustub {

/*
 * The base class defining a Matrix
 */
template <typename T>
class Matrix {
 protected:
  // TODO(P0): Add implementation
  Matrix(int r, int c) {
    rows = r >= 0 ? r : 0;
    cols = c >= 0 ? c : 0;
    if (rows * cols > 0) {
      linear = new T[rows * cols]();
    } else {
      linear = nullptr;
    }
  }

  // # of rows in the matrix
  int rows;
  // # of Columns in the matrix
  int cols;
  // Flattened array containing the elements of the matrix
  // TODO(P0) : Allocate the array in the constructor. Don't forget to free up
  // the array in the destructor.
  T *linear;

 public:
  // Return the # of rows in the matrix
  virtual int GetRows() = 0;

  // Return the # of columns in the matrix
  virtual int GetColumns() = 0;

  // Return the (i,j)th  matrix element
  virtual T GetElem(int i, int j) = 0;

  // Sets the (i,j)th  matrix element to val
  virtual void SetElem(int i, int j, T val) = 0;

  // Sets the matrix elements based on the array arr
  virtual void MatImport(T *arr) = 0;

  // TODO(P0): Add implementation
  virtual ~Matrix() {
      delete[] linear;
  }
};

template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  // TODO(P0): Add implementation
  RowMatrix(int r, int c) : Matrix<T>(r, c) {
    if (Matrix<T>::linear) {
      data_ = new T *[Matrix<T>::rows];
      for (int i = 0; i < Matrix<T>::rows; i++) {
        data_[i] = &Matrix<T>::linear[i * Matrix<T>::cols];
      }
    } else {
      data_ = nullptr;
    }
  }

  // TODO(P0): Add implementation
  int GetRows() override { return Matrix<T>::rows; }

  // TODO(P0): Add implementation
  int GetColumns() override { return Matrix<T>::cols; }

  // TODO(P0): Add implementation
  T GetElem(int i, int j) override {
    T result = T();
    if (i >= 0 && i < Matrix<T>::rows && j >= 0 && j < Matrix<T>::cols) {
      result = data_[i][j];
    }
    return result;
  }

  // TODO(P0): Add implementation
  void SetElem(int i, int j, T val) override {
    if (i >= 0 && i < Matrix<T>::rows && j >= 0 && j < Matrix<T>::cols) {
      data_[i][j] = val;
    } else {
      LOG_INFO("invalid index... %d , %d", i, j);
    }
  }

  // TODO(P0): Add implementation
  void MatImport(T *arr) override {
    if (arr){
      for (int i = 0; i < Matrix<T>::rows; i++) {
        for (int j = 0; j < Matrix<T>::cols; j++) {
          SetElem(i, j, arr[i * Matrix<T>::cols + j]);
        }
      }
    }
  }

  // TODO(P0): Add implementation
  ~RowMatrix() override {
      delete[] data_;
  }

 private:
  // 2D array containing the elements of the matrix in row-major format
  // TODO(P0): Allocate the array of row pointers in the constructor. Use these pointers
  // to point to corresponding elements of the 'linear' array.
  // Don't forget to free up the array in the destructor.
  T **data_;
};

template <typename T>
class RowMatrixOperations {
 public:
  // Compute (mat1 + mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> AddMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                   std::unique_ptr<RowMatrix<T>> mat2) {
    std::unique_ptr<RowMatrix<T>> result_ptr = std::unique_ptr<RowMatrix<T>>(nullptr);
    if (mat1 && mat2) {
      int r1 = mat1->GetRows();
      int c1 = mat1->GetColumns();
      int r2 = mat2->GetRows();
      int c2 = mat2->GetColumns();
      if (r1 == r2 && r1 != 0 && c1 == c2 && c1 != 0) {
        result_ptr.reset(new RowMatrix<T>(r1, c1));
        for (int i = 0; i < r1; i++) {
          for (int j = 0; j < c1; j++) {
            result_ptr->SetElem(i, j, mat1->GetElem(i, j) + mat2->GetElem(i, j));
          }
        }
      }
    }
    return result_ptr;
  }

  // Compute matrix multiplication (mat1 * mat2) and return the result.
  // Return nullptr if dimensions mismatch for input matrices.
  static std::unique_ptr<RowMatrix<T>> MultiplyMatrices(std::unique_ptr<RowMatrix<T>> mat1,
                                                        std::unique_ptr<RowMatrix<T>> mat2) {
    std::unique_ptr<RowMatrix<T>> result_ptr = std::unique_ptr<RowMatrix<T>>(nullptr);
    if (mat1 && mat2) {
      int r1 = mat1->GetRows();
      int c1 = mat1->GetColumns();
      int r2 = mat2->GetRows();
      int c2 = mat2->GetColumns();
      if (c1 == r2 && r1 != 0 && c1 != 0 && c2 != 0) {
        result_ptr.reset(new RowMatrix<T>(r1, c2));
        for (int i = 0; i < r1; i++) {
          for (int j = 0; j < c2; j++) {
            T sum = 0;
            for (int k = 0; k < c1; k++) {
              sum += mat1->GetElem(i, k) * mat2->GetElem(k, j);
            }
            result_ptr->SetElem(i, j, sum);
          }
        }
      }
    }
    return result_ptr;
  }

  // Simplified GEMM (general matrix multiply) operation
  // Compute (matA * matB + matC). Return nullptr if dimensions mismatch for input matrices
  static std::unique_ptr<RowMatrix<T>> GemmMatrices(std::unique_ptr<RowMatrix<T>> matA,
                                                    std::unique_ptr<RowMatrix<T>> matB,
                                                    std::unique_ptr<RowMatrix<T>> matC) {
    std::unique_ptr<RowMatrix<T>> result_ptr = std::unique_ptr<RowMatrix<T>>(nullptr);
    if (matA && matB && matC) {
      result_ptr = AddMatrices(MultiplyMatrices(matA, matB), matC);
    }
    return result_ptr;
  }
};
}  // namespace bustub
