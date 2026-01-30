import { describe, it, expect, beforeEach } from 'vitest';
import { Graph, P } from '../index.js';

describe('Predicates (P)', () => {
  let graph: Graph;

  beforeEach(() => {
    graph = new Graph();
    graph.addVertex('person', { name: 'Alice', age: 30n, score: 85.5 });
    graph.addVertex('person', { name: 'Bob', age: 25n, score: 92.0 });
    graph.addVertex('person', { name: 'Charlie', age: 35n, score: 78.0 });
    graph.addVertex('person', { name: 'Diana', age: 28n, score: 88.5 });
  });

  describe('Comparison Predicates', () => {
    it('P.eq() matches equal values', () => {
      const result = graph.V().hasWhere('age', P.eq(30n)).values('name').toList();
      expect(result).toEqual(['Alice']);
    });

    it('P.neq() matches non-equal values', () => {
      const result = graph.V().hasWhere('age', P.neq(30n)).values('name').toList();
      expect(result).toHaveLength(3);
      expect(result).not.toContain('Alice');
    });

    it('P.lt() matches less than', () => {
      const result = graph.V().hasWhere('age', P.lt(28n)).values('name').toList();
      expect(result).toEqual(['Bob']); // Only Bob is < 28
    });

    it('P.lte() matches less than or equal', () => {
      const result = graph.V().hasWhere('age', P.lte(28n)).values('name').toList();
      expect(result).toContain('Bob');
      expect(result).toContain('Diana');
      expect(result).toHaveLength(2);
    });

    it('P.gt() matches greater than', () => {
      const result = graph.V().hasWhere('age', P.gt(30n)).values('name').toList();
      expect(result).toEqual(['Charlie']); // Only Charlie is > 30
    });

    it('P.gte() matches greater than or equal', () => {
      const result = graph.V().hasWhere('age', P.gte(30n)).values('name').toList();
      expect(result).toContain('Alice');
      expect(result).toContain('Charlie');
      expect(result).toHaveLength(2);
    });
  });

  describe('Range Predicates', () => {
    it('P.between() matches inclusive start, exclusive end', () => {
      const result = graph.V().hasWhere('age', P.between(25n, 30n)).values('name').toList();
      // 25 <= age < 30, so Bob (25) and Diana (28)
      expect(result).toContain('Bob');
      expect(result).toContain('Diana');
      expect(result).not.toContain('Alice'); // 30 is excluded
    });

    it('P.inside() matches strictly inside range', () => {
      const result = graph.V().hasWhere('age', P.inside(25n, 35n)).values('name').toList();
      // 25 < age < 35, so Diana (28) and Alice (30)
      expect(result).toContain('Diana');
      expect(result).toContain('Alice');
      expect(result).not.toContain('Bob'); // 25 is excluded
      expect(result).not.toContain('Charlie'); // 35 is excluded
    });

    it('P.outside() matches outside range', () => {
      const result = graph.V().hasWhere('age', P.outside(26n, 34n)).values('name').toList();
      // age < 26 OR age > 34
      expect(result).toContain('Bob'); // 25 < 26
      expect(result).toContain('Charlie'); // 35 > 34
    });
  });

  describe('Collection Predicates', () => {
    it('P.within() matches values in set', () => {
      const result = graph.V().hasWhere('age', P.within([25n, 35n])).values('name').toList();
      expect(result).toContain('Bob');
      expect(result).toContain('Charlie');
      expect(result).toHaveLength(2);
    });

    it('P.without() matches values not in set', () => {
      const result = graph.V().hasWhere('age', P.without([25n, 35n])).values('name').toList();
      expect(result).toContain('Alice');
      expect(result).toContain('Diana');
      expect(result).not.toContain('Bob');
      expect(result).not.toContain('Charlie');
    });
  });

  describe('String Predicates', () => {
    it('P.containing() matches substring', () => {
      const result = graph.V().hasWhere('name', P.containing('li')).values('name').toList();
      expect(result).toContain('Alice');
      expect(result).toContain('Charlie');
    });

    it('P.notContaining() excludes substring', () => {
      const result = graph.V().hasWhere('name', P.notContaining('li')).values('name').toList();
      expect(result).toContain('Bob');
      expect(result).toContain('Diana');
      expect(result).not.toContain('Alice');
    });

    it('P.startingWith() matches prefix', () => {
      const result = graph.V().hasWhere('name', P.startingWith('A')).values('name').toList();
      expect(result).toEqual(['Alice']);
    });

    it('P.notStartingWith() excludes prefix', () => {
      const result = graph.V().hasWhere('name', P.notStartingWith('A')).values('name').toList();
      expect(result).not.toContain('Alice');
      expect(result).toHaveLength(3);
    });

    it('P.endingWith() matches suffix', () => {
      const result = graph.V().hasWhere('name', P.endingWith('b')).values('name').toList();
      expect(result).toEqual(['Bob']);
    });

    it('P.notEndingWith() excludes suffix', () => {
      const result = graph.V().hasWhere('name', P.notEndingWith('e')).values('name').toList();
      expect(result).toContain('Bob');
      expect(result).toContain('Diana');
      expect(result).not.toContain('Alice');
      expect(result).not.toContain('Charlie');
    });

    it('P.regex() matches regular expression', () => {
      const result = graph.V().hasWhere('name', P.regex('^[A-C].*')).values('name').toList();
      expect(result).toContain('Alice');
      expect(result).toContain('Bob');
      expect(result).toContain('Charlie');
      expect(result).not.toContain('Diana');
    });
  });

  describe('Logical Predicates', () => {
    it('P.and() combines predicates with AND', () => {
      const result = graph.V()
        .hasWhere('age', P.and(P.gte(25n), P.lte(30n)))
        .values('name')
        .toList();
      // 25 <= age <= 30
      expect(result).toContain('Alice');
      expect(result).toContain('Bob');
      expect(result).toContain('Diana');
      expect(result).not.toContain('Charlie');
    });

    it('P.or() combines predicates with OR', () => {
      const result = graph.V()
        .hasWhere('age', P.or(P.eq(25n), P.eq(35n)))
        .values('name')
        .toList();
      expect(result).toContain('Bob');
      expect(result).toContain('Charlie');
      expect(result).toHaveLength(2);
    });

    it('P.not() negates a predicate', () => {
      const result = graph.V()
        .hasWhere('age', P.not(P.lt(30n)))
        .values('name')
        .toList();
      // NOT (age < 30) means age >= 30
      expect(result).toContain('Alice');
      expect(result).toContain('Charlie');
      expect(result).not.toContain('Bob');
      expect(result).not.toContain('Diana');
    });
  });

  describe('Complex Predicate Combinations', () => {
    it('chains multiple predicates on different properties', () => {
      const result = graph.V()
        .hasWhere('age', P.gte(25n))
        .hasWhere('name', P.startingWith('A'))
        .values('name')
        .toList();
      expect(result).toEqual(['Alice']);
    });

    it('handles nested logical predicates', () => {
      // (age >= 30) AND (name starts with A OR name starts with C)
      const nameP = P.or(P.startingWith('A'), P.startingWith('C'));
      const result = graph.V()
        .hasWhere('age', P.gte(30n))
        .hasWhere('name', nameP)
        .values('name')
        .toList();
      expect(result).toContain('Alice');
      expect(result).toContain('Charlie');
      expect(result).toHaveLength(2);
    });
  });
});
