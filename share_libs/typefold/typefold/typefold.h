#pragma once

#include "share_headers/db_types.h"

// Fold functions convert all types to a lexicographical comparable format

// -------------------------------------------------------------------------------------
auto Fold(uint8_t *writer, const Integer &x) -> uint16_t;
auto Fold(uint8_t *writer, const UInteger &x) -> uint16_t;
auto Fold(uint8_t *writer, const Timestamp &x) -> uint16_t;
auto Fold(uint8_t *writer, const UniqueID &x) -> uint16_t;
template <int Leng>
auto Fold(uint8_t *writer, const Varchar<Leng> &x) -> uint16_t;

// -------------------------------------------------------------------------------------
auto Unfold(const uint8_t *input, Integer &x) -> uint16_t;
auto Unfold(const uint8_t *input, UInteger &x) -> uint16_t;
auto Unfold(const uint8_t *input, Timestamp &x) -> uint16_t;
auto Unfold(const uint8_t *input, UniqueID &x) -> uint16_t;
template <int Leng>
auto Unfold(const uint8_t *input, Varchar<Leng> &x) -> uint16_t;
