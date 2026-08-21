#[path = "../src/corpus.rs"]
mod corpus;

#[test]
fn fixture_is_byte_exact_and_targets_do_not_overlap() {
    let one = corpus::generate(0, 4, 2, 3, 4);
    let two = corpus::generate(0, 4, 2, 3, 4);
    assert_eq!(one, two);
    assert_eq!(
        corpus::canonical_input_json(&one),
        br#"{"source":[{"id":0,"group_id":0,"dimension_id":1,"value":226619,"active":true},{"id":1,"group_id":2,"dimension_id":1,"value":-876592,"active":true},{"id":2,"group_id":0,"dimension_id":0,"value":275505,"active":true},{"id":3,"group_id":2,"dimension_id":1,"value":706682,"active":true}],"dimension":[[0,2],[1,0]],"changes":[{"Insert":{"after":{"id":4,"group_id":0,"dimension_id":0,"value":388605,"active":true}}},{"Insert":{"after":{"id":5,"group_id":2,"dimension_id":1,"value":188320,"active":false}}},{"Update":{"before":{"id":0,"group_id":0,"dimension_id":1,"value":226619,"active":true},"after":{"id":0,"group_id":0,"dimension_id":1,"value":-481239,"active":false}}},{"Delete":{"before":{"id":1,"group_id":2,"dimension_id":1,"value":-876592,"active":true}}}]}"#
    );
    assert_eq!(
        corpus::canonical_changes_json(&one.changes),
        corpus::canonical_changes_json(&two.changes)
    );
}
