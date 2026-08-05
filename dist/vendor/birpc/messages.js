/**
 * Vendored from antfu-collective/birpc at reviewed commit 6b891740de348a82c69dbd38310d1ec822c7640b (v4.0.0).
 * MIT License, Copyright (c) 2021 Anthony Fu <https://github.com/antfu>.
 * Distributed under the MIT License. See THIRD_PARTY_NOTICES.md at repository root.
 * This file matches that commit except for one mechanical change: relative import paths
 * were given explicit .js extensions for NodeNext module resolution. It is
 * imported only as an internal RPC dispatcher; it is not part of the public interface.
 */
export const TYPE_REQUEST = 'q';
export const TYPE_RESPONSE = 's';
