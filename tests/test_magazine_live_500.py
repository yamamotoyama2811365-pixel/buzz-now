from pathlib import Path
import ast
import unittest

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT / 'app/main.py'


class MagazineLive500RegressionTest(unittest.TestCase):
    def test_three_points_inputs_are_initialized_first(self):
        text = MAIN.read_text()
        editorial = '    editorial_brief = editorial.load_brief(db, trend["id"])\n'
        briefing = '    briefing = _article_briefing(sources, trend["keyword"])\n'
        call = '    buzz_points = magazine.three_points(trend, briefing, editorial_brief)\n'
        self.assertIn(editorial, text)
        self.assertIn(briefing, text)
        self.assertIn(call, text)
        self.assertLess(text.index(editorial), text.index(call))
        self.assertLess(text.index(briefing), text.index(call))
        ast.parse(text)


if __name__ == '__main__':
    unittest.main()
