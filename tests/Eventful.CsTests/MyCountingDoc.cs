﻿using System;

namespace Eventful.CsTests
{
    public class MyCountingDoc
    {
        public Guid Id { get; set; }
        public int Count { get; set; }
        public int Value { get; set; }
        public int Writes { get; set; }
        public string Foo { get; set; }
    }

    public class MyPermissionDoc
    {
        public Guid Id { get; set; }
        public int Writes { get; set; }
    }
}