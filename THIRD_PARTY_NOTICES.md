# Third-Party Notices

## birpc (vendored)

`src/vendor/birpc/{main,messages,utils}.ts` are vendored from the
[`antfu-collective/birpc`](https://github.com/antfu-collective/birpc) repository
at reviewed commit `6b891740de348a82c69dbd38310d1ec822c7640b` (release `v4.0.0`).

- License: MIT
- Copyright (c) 2021 Anthony Fu <https://github.com/antfu>
- These files match that commit **except for one mechanical change**: relative import
  paths were given explicit `.js` extensions so the package builds under NodeNext module
  resolution (verified against `/tmp/birpc-audit` at the time of vendoring).

### Why vendored instead of a normal dependency

The upstream Git checkout omits its generated `dist/` directory (it is git-ignored
and built only by `prepublishOnly`), so a raw Git dependency cannot be imported
without an install-time build step. Vendoring the reviewed, unmodified source
keeps the package importable from committed `dist` with **zero runtime third-party
dependencies** and no consumer-time `prepare`/build. `birpc` is used only as an
internal RPC dispatcher and is not part of this package's public interface.

### License text

MIT License

Copyright (c) 2021 Anthony Fu <https://github.com/antfu>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## MIT License (this package)

This package is distributed under the MIT License (see `LICENSE`).
