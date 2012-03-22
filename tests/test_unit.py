import unittest

from scrapinghub import urlencode, to_str


class UtilsTest(unittest.TestCase):

    def test_urlencode(self):
        self.assertEqual(urlencode({}), '')
        self.assertEqual(urlencode(()), '')
        self.assertEqual(urlencode(dict(foo='bar', bar='foo')),
                         'foo=bar&bar=foo')
        self.assertEqual(urlencode([('foo', 'bar'), ('bar', 'foo')]),
                         'foo=bar&bar=foo')

    def test_urlencode_unicode(self):
        self.assertEqual(urlencode([(u'\xf1', u'\u20ac')]),
                         '%C3%B1=%E2%82%AC')

    def test_urlencode_no_doseq(self):
        self.assertEqual(urlencode(dict(foo=[1])),
                                   'foo=%5B%271%27%5D')

    def test_urlencode_doseq(self):
        self.assertEqual(urlencode(dict(foo=[1,2,3]), True),
                         'foo=1&foo=2&foo=3')

    def test_urlencode_doseq_unicode(self):
        self.assertEqual(urlencode([(u'\xf1', [u'\xe1', u'\xe9', 'i'])], True),
                         '%C3%B1=%C3%A1&%C3%B1=%C3%A9&%C3%B1=i')

    def test_to_str(self):
        self.assertEqual(to_str([]), '[]')
        self.assertEqual(to_str(1), '1')

    def test_to_str_unicode(self):
        self.assertEqual(to_str(u'\xf1'), '\xc3\xb1')
        self.assertEqual(to_str(u'fuho', 'rot13'), 'shub')



if __name__ == '__main__':
    unittest.main()
